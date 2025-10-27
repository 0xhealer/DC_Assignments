use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    cmp::Reverse,
    fs::OpenOptions,
    io::{Write},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tiny_http::{Server, Response};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct RequestMsg {
    from: usize,
    ts: u64,
    resource: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct ReplyMsg {
    from: usize,
    resource: String,
}

#[derive(Clone)]
struct Node {
    id: usize,
    port: u16,
    peers: Vec<(usize, u16)>,
    state: Arc<Mutex<State>>,
    client: Client,
    log_file: Arc<Mutex<std::fs::File>>,
}

#[derive(Debug)]
struct State {
    timestamp: u64,
    request_queues: HashMap<String, BinaryHeap<Reverse<(u64, usize)>>>,
    replies: HashMap<String, HashSet<usize>>,
}

impl Node {
    fn new(id: usize, port: u16, peers: Vec<(usize, u16)>, log_file: Arc<Mutex<std::fs::File>>) -> Self {
        let mut rq = HashMap::new();
        rq.insert("A".to_string(), BinaryHeap::new());
        rq.insert("B".to_string(), BinaryHeap::new());
        let mut reps = HashMap::new();
        reps.insert("A".to_string(), HashSet::new());
        reps.insert("B".to_string(), HashSet::new());

        Self {
            id,
            port,
            peers,
            state: Arc::new(Mutex::new(State {
                timestamp: 0,
                request_queues: rq,
                replies: reps,
            })),
            client: Client::new(),
            log_file,
        }
    }
    fn log(&self, msg: &str) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let line = format!("[{}] [Node {}] {}\n", now, self.id, msg);
        if !msg.contains("Timeout waiting for replies") {
            print!("{}", line);
        }
        if let Ok(mut f) = self.log_file.lock() {
            let _ = f.write_all(line.as_bytes());
            let _ = f.flush();
        }
    }

    fn start_server(&self) {
        let port = self.port;
        let node = self.clone();
        thread::spawn(move || {
            let addr = format!("0.0.0.0:{}", port);
            let server = Server::http(&addr).unwrap();
            node.log(&format!("Server started on {}", addr));
            for mut req in server.incoming_requests() {
                let url = req.url().to_string();
                let mut content = String::new();
                let _ = req.as_reader().read_to_string(&mut content);
                if url == "/receive_request" {
                    if let Ok(msg) = serde_json::from_str::<RequestMsg>(&content) {
                        node.receive_request(msg);
                    } else {
                        node.log(&format!("Bad REQUEST payload: {}", content));
                    }
                } else if url == "/receive_reply" {
                    if let Ok(msg) = serde_json::from_str::<ReplyMsg>(&content) {
                        node.receive_reply(msg);
                    } else {
                        node.log(&format!("Bad REPLY payload: {}", content));
                    }
                }
                let _ = req.respond(Response::from_string("OK"));
            }
        });
    }

    fn broadcast_request(&self, resource: &str) {
        {
            let mut st = self.state.lock().unwrap();
            st.timestamp += 1;
            let ts = st.timestamp;
            if let Some(q) = st.request_queues.get_mut(resource) {
                q.push(Reverse((ts, self.id)));
            }
            if let Some(rset) = st.replies.get_mut(resource) {
                rset.clear();
            }
        }

        let ts = {
            let st = self.state.lock().unwrap();
            st.timestamp
        };

        self.log(&format!("Broadcasting REQUEST ts={} for resource={}", ts, resource));
        let payload = serde_json::to_string(&RequestMsg { from: self.id, ts, resource: resource.to_string() }).unwrap();

        for (nid, port) in &self.peers {
            let url = format!("http://127.0.0.1:{}/receive_request", port);
            let client = self.client.clone();
            let node = self.clone();
            let payload_clone = payload.clone();
            let nid_val = *nid;
            thread::spawn(move || {
                if let Err(e) = client.post(&url).body(payload_clone).send() {
                    node.log(&format!("Error sending REQUEST to {}: {}", nid_val, e));
                }
            });
        }
    }

    fn receive_request(&self, msg: RequestMsg) {
        {
            let mut st = self.state.lock().unwrap();
            st.timestamp = std::cmp::max(st.timestamp, msg.ts) + 1;
            if let Some(q) = st.request_queues.get_mut(&msg.resource) {
                q.push(Reverse((msg.ts, msg.from)));
            }
        }
        self.log(&format!("Received REQUEST from {} ts={} for resource={}", msg.from, msg.ts, msg.resource));
        if let Some((_nid, port)) = self.peers.iter().find(|(nid, _)| *nid == msg.from) {
            let url = format!("http://127.0.0.1:{}/receive_reply", port);
            let payload = serde_json::to_string(&ReplyMsg { from: self.id, resource: msg.resource.clone() }).unwrap();
            if let Err(e) = self.client.post(&url).body(payload).send() {
                self.log(&format!("Error sending REPLY to {}: {}", msg.from, e));
            }
        }
    }

    fn receive_reply(&self, msg: ReplyMsg) {
        let mut st = self.state.lock().unwrap();
        if let Some(set) = st.replies.get_mut(&msg.resource) {
            set.insert(msg.from);
        } else {
            let mut set = HashSet::new();
            set.insert(msg.from);
            st.replies.insert(msg.resource.clone(), set);
        }
        drop(st);
        self.log(&format!("Received REPLY from {} for resource={}", msg.from, msg.resource));
    }

    fn can_enter_cs(&self, resource: &str) -> bool {
        let st = self.state.lock().unwrap();
        if let Some(q) = st.request_queues.get(resource) {
            if let Some(Reverse((_, nid))) = q.peek().cloned() {
                let rcount = st.replies.get(resource).map(|s| s.len()).unwrap_or(0);
                return nid == self.id && rcount >= self.peers.len();
            }
        }
        false
    }

    fn enter_cs(&self, resource: &str) {
        self.broadcast_request(resource);
        let start = SystemTime::now();
        loop {
            if self.can_enter_cs(resource) {
                self.log(&format!("Entering Critical Section for resource={}", resource));
                thread::sleep(Duration::from_millis(500));
                self.log(&format!("Exiting Critical Section for resource={}", resource));
                let mut st = self.state.lock().unwrap();
                if let Some(q) = st.request_queues.get_mut(resource) {
                    if let Some(Reverse((_, nid))) = q.peek().cloned() {
                        if nid == self.id {
                            let _ = q.pop();
                        } else {
                            let mut items = vec![];
                            while let Some(Reverse(entry)) = q.pop() {
                                items.push(entry);
                            }
                            items.retain(|&(_t, node)| node != self.id);
                            for entry in items {
                                q.push(Reverse(entry));
                            }
                        }
                    }
                }
                if let Some(rset) = st.replies.get_mut(resource) {
                    rset.clear();
                }
                drop(st);
                break;
            }
            if SystemTime::now().duration_since(start).unwrap().as_secs() > 6 {
                self.log("Timeout waiting for replies");
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

fn main() {
    let nodes = vec![(0, 8000), (1, 8001), (2, 8002), (3, 8003)];
    let log_file = Arc::new(Mutex::new(
        OpenOptions::new().create(true).append(true).open("lamport.log").unwrap(),
    ));

    let mut handles = vec![];
    for (id, port) in nodes.clone() {
        let peers = nodes.iter().filter(|(nid, _)| *nid != id).cloned().collect::<Vec<_>>();
        let node = Node::new(id, port, peers, log_file.clone());
        node.start_server();

        let n = node.clone();
        let h = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1 + id as u64));
            n.enter_cs("A");
            thread::sleep(Duration::from_millis(200 + (id as u64 * 100)));
            n.enter_cs("B");
            thread::sleep(Duration::from_secs(1));
        });
        handles.push(h);
    }

    for h in handles {
        let _ = h.join();
    }
}


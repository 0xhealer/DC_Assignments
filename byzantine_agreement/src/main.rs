use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use tiny_http::{Response, Server};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct OrderMsg {
    from: usize,
    order: String,
}

struct Node {
    id: usize,
    port: u16,
    peers: Vec<(usize, u16)>,
    is_byzantine: bool,
    commander_order: Arc<Mutex<Option<String>>>,
    forwarded: Arc<Mutex<HashMap<usize, String>>>,
    client: Client,
    log_file: Arc<Mutex<std::fs::File>>,
    decided: Arc<Mutex<Option<String>>>,
}

impl Node {
    fn new(id: usize, port: u16, peers: Vec<(usize, u16)>, is_byzantine: bool, log_file: Arc<Mutex<std::fs::File>>) -> Self {
        Node {
            id,
            port,
            peers,
            is_byzantine,
            commander_order: Arc::new(Mutex::new(None)),
            forwarded: Arc::new(Mutex::new(HashMap::new())),
            client: Client::new(),
            log_file,
            decided: Arc::new(Mutex::new(None)),
        }
    }

    fn log(&self, msg: &str) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let line = format!("[{}] [Node {}] {}\n", now, self.id, msg);
        print!("{}", line);
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
            let server = Server::http(&addr).expect("failed to start tiny-http server");
            node.log(&format!("HTTP server listening on {}", addr));
            for mut req in server.incoming_requests() {
                let url = req.url().to_string();
                let mut body = String::new();
                let _ = req.as_reader().read_to_string(&mut body);
                if url == "/order" {
                    if let Ok(msg) = serde_json::from_str::<OrderMsg>(&body) {
                        node.receive_order(msg);
                    } else {
                        node.log(&format!("Bad /order payload: {}", body));
                    }
                } else if url == "/forward" {
                    if let Ok(msg) = serde_json::from_str::<OrderMsg>(&body) {
                        node.receive_forward(msg);
                    } else {
                        node.log(&format!("Bad /forward payload: {}", body));
                    }
                }
                let _ = req.respond(Response::from_string("OK"));
            }
        });
    }

    fn commander_send(&self, order_map: &HashMap<usize, String>) {
        for (nid, port) in &self.peers {
            let order = order_map.get(nid).cloned().unwrap_or_else(|| "RETREAT".to_string());
            let url = format!("http://127.0.0.1:{}/order", port);
            let payload = serde_json::to_string(&OrderMsg { from: self.id, order }).unwrap();
            let client = self.client.clone();
            let nidv = *nid;
            let node = self.clone();
            thread::spawn(move || {
                if let Err(e) = client.post(&url).body(payload).send() {
                    node.log(&format!("Error sending ORDER to {}: {}", nidv, e));
                } else {
                    node.log(&format!("Sent ORDER to {} (via /order)", nidv));
                }
            });
        }
    }

    fn receive_order(&self, msg: OrderMsg) {
        self.log(&format!("Received ORDER from commander {}: {}", msg.from, msg.order));
        {
            let mut c = self.commander_order.lock().unwrap();
            *c = Some(msg.order.clone());
        }
        self.forward_order(msg.order);
    }

    fn forward_order(&self, order: String) {
        let to_send = if self.is_byzantine {
            if order == "ATTACK" { "RETREAT".to_string() } else { "ATTACK".to_string() }
        } else {
            order.clone()
        };
        {
            let mut f = self.forwarded.lock().unwrap();
            f.insert(self.id, to_send.clone());
        }
        for (nid, port) in &self.peers {
            if *nid == self.id { continue; }
            let url = format!("http://127.0.0.1:{}/forward", port);
            let payload = serde_json::to_string(&OrderMsg { from: self.id, order: to_send.clone() }).unwrap();
            let client = self.client.clone();
            let node = self.clone();
            let nidv = *nid;
            thread::spawn(move || {
                if let Err(e) = client.post(&url).body(payload).send() {
                    node.log(&format!("Error forwarding to {}: {}", nidv, e));
                } else {
                    node.log(&format!("Forwarded order to {} via /forward", nidv));
                }
            });
        }
    }

    fn receive_forward(&self, msg: OrderMsg) {
        self.log(&format!("Received FORWARD from {}: {}", msg.from, msg.order));
        {
            let mut f = self.forwarded.lock().unwrap();
            f.insert(msg.from, msg.order.clone());
        }
    }

    fn decide(&self) -> Option<String> {
        thread::sleep(Duration::from_millis(500));
        let commander_opt = { self.commander_order.lock().unwrap().clone() };
        let forwarded_map = { self.forwarded.lock().unwrap().clone() };

        if commander_opt.is_none() {
            self.log("No commander order received yet; cannot decide");
            return None;
        }
        let mut counts: HashMap<String, usize> = HashMap::new();
        let cmd = commander_opt.unwrap();
        *counts.entry(cmd.clone()).or_insert(0) += 1;
        for (_from, ord) in forwarded_map.iter() {
            *counts.entry(ord.clone()).or_insert(0) += 1;
        }
        let mut best = None;
        let mut bestc = 0usize;
        for (k, v) in counts {
            if v > bestc {
                best = Some(k);
                bestc = v;
            }
        }
        best
    }
}

impl Clone for Node {
    fn clone(&self) -> Self {
        Node {
            id: self.id,
            port: self.port,
            peers: self.peers.clone(),
            is_byzantine: self.is_byzantine,
            commander_order: Arc::clone(&self.commander_order),
            forwarded: Arc::clone(&self.forwarded),
            client: self.client.clone(),
            log_file: Arc::clone(&self.log_file),
            decided: Arc::clone(&self.decided),
        }
    }
}

fn main() {
    let nodes = vec![(0, 8000), (1, 8001), (2, 8002)];
    let byzantine_nodes = vec![2usize];

    let log_file = Arc::new(Mutex::new(
        OpenOptions::new().create(true).append(true).open("byzantine.log").unwrap(),
    ));

    let mut node_objs: HashMap<usize, Node> = HashMap::new();
    for (id, port) in nodes.iter() {
        let peers = nodes.iter().filter(|(nid, _)| nid != id).cloned().collect::<Vec<_>>();
        let is_byz = byzantine_nodes.contains(id);
        let n = Node::new(*id, *port, peers, is_byz, log_file.clone());
        n.start_server();
        node_objs.insert(*id, n);
    }

    thread::sleep(Duration::from_millis(300));

    let commander = node_objs.get(&0).unwrap().clone();
    let mut order_map: HashMap<usize, String> = HashMap::new();
    for (nid, _port) in nodes.iter() {
        order_map.insert(*nid, "ATTACK".to_string());
    }
    commander.commander_send(&order_map);

    thread::sleep(Duration::from_secs(1));

    for id in [1usize, 2usize] {
        if let Some(node) = node_objs.get(&id) {
            let dec = node.decide();
            if let Some(v) = dec {
                node.log(&format!("FINAL DECISION = {}", v));
                let mut d = node.decided.lock().unwrap();
                *d = Some(v);
            } else {
                node.log("FINAL DECISION = None");
            }
        }
    }

    thread::sleep(Duration::from_millis(200));
}


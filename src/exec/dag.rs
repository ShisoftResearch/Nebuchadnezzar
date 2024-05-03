use std::collections::HashMap;

use dovahkiin::expr::serde::Expr;
use serde::{Deserialize, Serialize};

use super::symbols::Symbol;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    id: u32,
    symbol: Symbol,
    params: Expr
}

#[derive(Serialize, Deserialize)]
pub struct DAG {
    nodes: Vec<Node>,
    outlinks: HashMap<u32, Vec<u32>>,
    inlinks: HashMap<u32, Vec<u32>>
}

impl DAG {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            outlinks: HashMap::new(),
            inlinks: HashMap::new(),
        }
    }

    pub fn push_node(&mut self, symbol: Symbol, params: Expr) -> &Node {
        let id = self.nodes.len() as u32;
        let node = Node {
            id, symbol, params
        };
        self.nodes.push(node.clone());
        self.nodes.last().unwrap()
    }

    pub fn link(&mut self, from: u32, to: u32) {
        self.outlinks.entry(from).or_insert_with(|| vec![]).push(to);
        self.inlinks.entry(to).or_insert_with(|| vec![]).push(from);
    }

    pub fn outlinks(&self, id: u32) -> &Vec<u32> {
        &self.outlinks[&id]
    }

    pub fn inlinks(&self, id: u32) -> &Vec<u32> {
        &self.inlinks[&id]
    }

    pub fn topological_sort(&self) -> Result<Vec<u32>, String> {
        // Create a copy of the inlinks to manipulate
        let mut in_degree = self.inlinks.clone();

        // Vector to store the nodes with zero in-degree (no incoming edges)
        let mut queue: Vec<u32> = in_degree.iter()
            .filter(|(_, links)| links.is_empty())
            .map(|(node, _)| *node)
            .collect();

        // If queue is initially empty, and there are nodes, there's a cycle
        if queue.is_empty() && !self.nodes.is_empty() {
            return Err("Graph has cycles, cannot perform topological sort".to_string());
        }

        let mut sorted = Vec::new();

        while let Some(node) = queue.pop() {
            sorted.push(node);

            // For each node `m` that `node` connects to...
            if let Some(outgoing) = self.outlinks.get(&node) {
                for &m in outgoing {
                    // Remove the edge from the node to m
                    if let Some(links) = in_degree.get_mut(&m) {
                        links.retain(|&x| x != node);

                        // If `m` has no other incoming edges, add it to the queue
                        if links.is_empty() {
                            queue.push(m);
                        }
                    }
                }
            }
        }

        // Check if we've processed all nodes (sorted should have exactly the same number of nodes as in the original graph)
        if sorted.len() == self.nodes.len() {
            Ok(sorted)
        } else {
            Err("Graph has cycles, cannot perform topological sort".to_string())
        }
    }
}
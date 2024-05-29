// This is the general representation of DAG
// It can be used on local execution and distributed execution
// depends on the model

use std::collections::{HashMap, VecDeque};

use dovahkiin::expr::serde::Expr;
use serde::{Deserialize, Serialize};

use super::symbols::*;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    id: u32,
    symbol: NebSymbol,
    params: Expr,
}

#[derive(Serialize, Deserialize)]
pub struct DAG {
    nodes: Vec<Node>,
    outlinks: HashMap<u32, Vec<u32>>,
    inlinks: HashMap<u32, Vec<u32>>,
}

impl DAG {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            outlinks: HashMap::new(),
            inlinks: HashMap::new(),
        }
    }

    pub fn push_node(&mut self, symbol: NebSymbol, params: Expr) -> &Node {
        let id = self.nodes.len() as u32;
        let node = Node { id, symbol, params };
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
        let mut in_degree: HashMap<u32, usize> = HashMap::new();
        let mut zero_in_degree_queue: VecDeque<u32> = VecDeque::new();
        let mut topo_sorted: Vec<u32> = Vec::new();

        // Initialize in-degree of all nodes to 0
        for node in &self.nodes {
            in_degree.insert(node.id, 0);
        }

        // Calculate in-degrees based on the inlinks
        for (node_id, dependencies) in &self.inlinks {
            *in_degree.get_mut(node_id).unwrap() = dependencies.len();
        }

        // Collect nodes with zero in-degree
        for (&node_id, &deg) in &in_degree {
            if deg == 0 {
                zero_in_degree_queue.push_back(node_id);
            }
        }

        // Process nodes with zero in-degree
        while let Some(node_id) = zero_in_degree_queue.pop_front() {
            topo_sorted.push(node_id);

            // Reduce in-degree of child nodes and add new zero in-degree nodes to the queue
            if let Some(children) = self.outlinks.get(&node_id) {
                for &child_id in children {
                    let child_in_degree = in_degree.get_mut(&child_id).unwrap();
                    *child_in_degree -= 1;
                    if *child_in_degree == 0 {
                        zero_in_degree_queue.push_back(child_id);
                    }
                }
            }
        }

        //return Ok(topo_sorted);

        // Check if topological sort was successful
        if topo_sorted.len() == self.nodes.len() {
            Ok(topo_sorted)
        } else {
            Err("Graph has cycles, cannot perform topological sort".to_string())
        }
    }

    pub fn group_into_stages(&self, topo_sorted: Vec<u32>) -> Vec<Vec<Vec<Node>>> {
        let mut stages: Vec<Vec<Vec<Node>>> = Vec::new();
        let mut node_to_stage: HashMap<u32, usize> = HashMap::new();
        let mut node_map: HashMap<u32, Node> = HashMap::new();

        // Create a map from node ID to Node for easy access
        for node in &self.nodes {
            node_map.insert(node.id, node.clone());
        }

        // Process each node in topologically sorted order
        for &node_id in &topo_sorted {
            let mut stage = 0;

            // Determine the stage based on dependencies
            if let Some(dependencies) = self.inlinks.get(&node_id) {
                for &dep in dependencies {
                    if let Some(&dep_stage) = node_to_stage.get(&dep) {
                        // Ensure the node is placed in the next stage after its dependencies
                        stage = stage.max(dep_stage + 1);
                    }
                }
            }

            // Ensure the stages vector has enough stages
            if stages.len() <= stage {
                stages.push(Vec::new());
            }

            // Check if the node should start a new group or continue the last group in the stage
            let mut new_group = true;
            if let Some(last_group) = stages[stage].last_mut() {
                let last_node = last_group.last().unwrap();
                if let Some(children) = self.outlinks.get(&last_node.id) {
                    if children.contains(&node_id) {
                        last_group.push(node_map[&node_id].clone());
                        new_group = false;
                    }
                }
            }

            // Create a new group if necessary
            if new_group {
                stages[stage].push(vec![node_map[&node_id].clone()]);
            }

            // Update the node to stage map
            node_to_stage.insert(node_id, stage);
        }

        stages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_nodes_and_edges() {
        let mut dag = DAG::new();
        let node0 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node1 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node2 = dag.push_node(NebSymbol::All, Expr::nothing()).id;

        dag.link(node0, node1);
        dag.link(node1, node2);

        assert_eq!(dag.nodes.len(), 3);
        assert_eq!(dag.outlinks[&node0], vec![node1]);
        assert_eq!(dag.inlinks[&node1], vec![node0]);
        assert_eq!(dag.outlinks[&node1], vec![node2]);
        assert_eq!(dag.inlinks[&node2], vec![node1]);
    }

    #[test]
    fn test_topological_sort() {
        let mut dag = DAG::new();
        let node0 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node1 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node2 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node3 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node4 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node5 = dag.push_node(NebSymbol::All, Expr::nothing()).id;

        dag.link(node0, node1);
        dag.link(node0, node2);
        dag.link(node1, node3);
        dag.link(node2, node3);
        dag.link(node3, node4);
        dag.link(node4, node5);

        let topo_sorted = dag.topological_sort().unwrap();
        assert_eq!(topo_sorted, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_group_into_stages() {
        let mut dag = DAG::new();
        let node0 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node1 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node2 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node3 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node4 = dag.push_node(NebSymbol::All, Expr::nothing()).id;
        let node5 = dag.push_node(NebSymbol::All, Expr::nothing()).id;

        dag.link(node0, node1);
        dag.link(node0, node2);
        dag.link(node1, node3);
        dag.link(node2, node3);
        dag.link(node3, node4);
        dag.link(node4, node5);

        let topo_sorted = dag.topological_sort().unwrap();
        let stages = dag.group_into_stages(topo_sorted);

        assert_eq!(stages.len(), 5); // There should be 5 stages

        assert_eq!(stages[0].len(), 1);
        assert_eq!(stages[0][0][0].id, 0);

        assert_eq!(stages[1].len(), 2);
        assert_eq!(stages[1][0][0].id, 1);
        assert_eq!(stages[1][1][0].id, 2);

        assert_eq!(stages[2].len(), 1);
        assert_eq!(stages[2][0][0].id, 3);

        assert_eq!(stages[3].len(), 1);
        assert_eq!(stages[3][0][0].id, 4);

        assert_eq!(stages[4].len(), 1);
        assert_eq!(stages[4][0][0].id, 5);
    }
}
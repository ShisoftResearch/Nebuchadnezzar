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

    pub fn group_into_stages(&self, topo_sorted: Vec<u32>) -> Vec<Vec<Vec<u32>>> {
        let mut stages: Vec<Vec<Vec<u32>>> = Vec::new();
        let mut node_stage: Vec<i32> = vec![-1; self.nodes.len()];

        for &node_id in topo_sorted.iter() {
            let parent = self.inlinks.get(&node_id);
            let (parent_id, parent_stage) = parent
                .map(|pids| {
                    pids.iter()
                        .map(|id| (*id as i32, node_stage[*id as usize]))
                        .max_by_key(|(_, stage)| *stage)
                        .unwrap()
                })
                .unwrap_or((-1, -1));
            if let Some(node) = self.nodes.get(node_id as usize) {
                if node.symbol.symbol_type() == SymbolType::Partitioning || parent_stage == -1 {
                    // New stage
                    let next_stage = parent_stage + 1;
                    if stages.len() as i32 <= next_stage {
                        // No need a loop to ensure the stage exists
                        // The new stage will always be the next one
                        stages.push(vec![]);
                    }
                    let current_stage = &mut stages[next_stage as usize];
                    current_stage.push(vec![node_id]);
                    node_stage[node_id as usize] = next_stage;
                } else {
                    // Add to old stage as where its parent in
                    if parent_stage == -1 {}
                    let current_stage = &mut stages[parent_stage as usize];
                    // Search for the thread of its parent
                    let thread = current_stage
                        .iter_mut()
                        .find(|th| *th.last().unwrap() as i32 == parent_id);
                    if let Some(thread) = thread {
                        thread.push(node_id);
                    } else {
                        // If cannot find one, assign to a new thread
                        current_stage.push(vec![node_id]);
                    }
                    node_stage[node_id as usize] = parent_stage;
                }
            }
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
        // In this case we will use a most typical cell query statement
        // (filter-shared-value
        //    (id-cell-sel
        //      (cell-id-query SCHEMA)
        //     FIELDS)
        //   FILTER)
        // The DAG of this statement would be linear with 2 stages
        // Stage 1: Query cell ids from the index
        // Stage 2: Partition cell ids with consistent hash and then --
        //          Get cells of the ids, then
        //          Filter the cells
        // There would be one thread in both stages
        // Stage 2 would have 2 operations, which is 'id-cell-sel' and 'filter-shared-value'
        let mut dag = DAG::new();
        let node0 = dag.push_node(NebSymbol::CellIdQuery, Expr::nothing()).id;
        let node1 = dag.push_node(NebSymbol::IdCellSel, Expr::nothing()).id;
        let node2 = dag
            .push_node(NebSymbol::FilterSharedValue, Expr::nothing())
            .id;

        dag.link(node0, node1);
        dag.link(node1, node2);

        let topo_sorted = dag.topological_sort().unwrap();
        let stages = dag.group_into_stages(topo_sorted);

        assert_eq!(stages.len(), 2); // There should be 2 stages

        assert_eq!(stages[0].len(), 1);
        assert_eq!(stages[0][0].len(), 1);
        assert_eq!(stages[0][0][0], 0);

        assert_eq!(stages[1].len(), 1, "Having stages {:?}", stages);
        assert_eq!(stages[1][0].len(), 2);
        assert_eq!(stages[1][0][0], 1);
        assert_eq!(stages[1][0][1], 2);
    }
}

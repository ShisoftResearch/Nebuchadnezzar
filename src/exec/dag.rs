// This is the general representation of DAG
// It can be used on local execution and distributed execution
// depends on the model

use std::collections::{BTreeMap, HashMap, VecDeque};

use dovahkiin::expr::serde::Expr;
use dovahkiin::types::OwnedValue;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use super::{
    query::{env::Environment, expand::Expand},
    symbols::*,
};

pub type Stages = Vec<Vec<Thread>>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    id: u32,
    symbol: NebSymbol,

    params: Vec<Expr>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DAG {
    nodes: Vec<Node>,
    outlinks: HashMap<u32, Vec<u32>>,
    inlinks: HashMap<u32, Vec<u32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Thread {
    id: u32,
    partitioned: bool,
    remote_src: bool,
    nodes: Vec<u32>,
}

impl DAG {
    pub fn new() -> Self {
        Self {
            nodes: vec![],
            outlinks: HashMap::new(),
            inlinks: HashMap::new(),
        }
    }

    pub fn push_node(&mut self, symbol: NebSymbol, params: Vec<Expr>) -> &mut Node {
        let id = self.nodes.len() as u32 + 1;
        let node = Node { id, symbol, params };
        self.nodes.push(node.clone());
        self.nodes.last_mut().unwrap()
    }

    pub fn get_node(&self, node_id: u32) -> Option<&Node> {
        self.nodes.get(node_id as usize - 1)
    }

    pub fn get_node_mut(&mut self, node_id: u32) -> Option<&mut Node> {
        self.nodes.get_mut(node_id as usize - 1)
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

    pub fn rev_topological_sort(&self) -> Vec<u32> {
        let mut out_degree: BTreeMap<u32, usize> = BTreeMap::new();
        let mut zero_out_degree_queue: VecDeque<u32> = VecDeque::new();
        let mut topo_sorted: Vec<u32> = Vec::new();

        // Initialize out-degree of all nodes to 0
        for node in &self.nodes {
            out_degree.insert(node.id, 0);
        }

        // Calculate out-degrees based on the outlinks
        for (to, froms) in self.outlinks.iter() {
            out_degree.get_mut(to).map(|n| *n = froms.len());
        }

        // Collect nodes with zero out-degree (leaf nodes)
        for (&node_id, &deg) in &out_degree {
            if deg == 0 {
                zero_out_degree_queue.push_back(node_id);
            }
        }

        // Process nodes with zero out-degree
        while let Some(node_id) = zero_out_degree_queue.pop_front() {
            topo_sorted.push(node_id);

            // Reduce out-degree of parent nodes and add new zero out-degree nodes to the queue
            if let Some(parents) = self.inlinks.get(&node_id) {
                for &parent_id in parents {
                    let parent_out_degree = out_degree.get_mut(&parent_id).unwrap();
                    *parent_out_degree -= 1;
                    if *parent_out_degree == 0 {
                        zero_out_degree_queue.push_back(parent_id);
                    }
                }
            }
        }

        // Check if topological sort was successful
        assert_eq!(topo_sorted.len(), self.nodes.len());

        return topo_sorted;
    }

    pub fn stages(&self) -> Stages {
        let topo_sorted = self.rev_topological_sort();
        return self.group_into_stages(topo_sorted);
    }

    pub fn group_into_stages(&self, rev_topo_sorted: Vec<u32>) -> Stages {
        let mut stages: Stages = vec![vec![]];
        let mut node_stage: Vec<i32> = vec![-1; self.nodes.len() + 1];

        for &node_id in rev_topo_sorted.iter() {
            let dependences = self.outlinks.get(&node_id);
            let (dep_id, dep_stage) = dependences
                .map(|pids| {
                    pids.iter()
                        .map(|id| (*id as i32, node_stage[*id as usize]))
                        .max_by_key(|(_, stage)| *stage)
                        .unwrap()
                })
                .unwrap_or((0, 0));
            if let Some(node) = self.get_node(node_id) {
                let current_stage = &mut stages[dep_stage as usize];
                let num_threads = current_stage.len() as u32;
                // Search for the thread of its parent
                let thread = current_stage
                    .iter_mut()
                    .find(|th| *th.nodes.last().unwrap() as i32 == dep_id);
                let is_partitioner = node.symbol.symbol_type() == SymbolType::Partitioning;
                let mut need_new_stage = false;
                if let Some(thread) = thread {
                    let thread_partitioned = thread.partitioned;
                    if thread_partitioned && is_partitioner {
                        // Only move to next stage for this lineage when
                        // The thread already have a partitioner
                        need_new_stage = true;
                    } else {
                        thread.nodes.push(node_id);
                        thread.partitioned = thread_partitioned | is_partitioner;
                    }
                } else {
                    // If cannot find one, assign to a new thread
                    current_stage.push(Thread {
                        nodes: vec![node_id],
                        partitioned: is_partitioner,
                        remote_src: dep_stage != 0,
                        id: num_threads,
                    });
                }
                if need_new_stage {
                    let partition_id = dep_stage + 1;
                    if stages.len() <= partition_id as _ {
                        // Need to init a new tage
                        stages.push(vec![]);
                    }
                    let stage = &mut stages[partition_id as usize];
                    stage.push(Thread {
                        nodes: vec![node_id],
                        partitioned: is_partitioner,
                        remote_src: true,
                        id: num_threads,
                    });
                    // Mark this node in the next stage of its predessor
                    node_stage[node_id as usize] = partition_id;
                } else {
                    // Mark this node in the same stage as its predessor
                    node_stage[node_id as usize] = dep_stage;
                }
            }
        }

        stages
    }

    pub fn from_exprs(exprs: Vec<Expr>, env: &mut Environment) -> Result<Self, String> {
        let mut dag = Self::new();
        for expr in exprs {
            let expr = expr.expand(env)?; // Expand first
            let mut id_counter = 0;
            dag.construct_from_expr(0, expr, &mut id_counter)?;
        }
        return Ok(dag);
    }

    fn construct_from_expr(
        &mut self,
        prev_id: u32,
        expr: Expr,
        id_counter: &mut u32,
    ) -> Result<Expr, String> {
        match expr {
            Expr::List(ele) => {
                if ele.is_empty() {
                    return Ok(Expr::List(ele));
                }
                let mut neb_symbol = None;
                let mut has_symbol = false;
                {
                    let first_expr = &ele[0];
                    if let Expr::Symbol(sym_id, _) = first_expr {
                        neb_symbol = neb_id_symbol(*sym_id);
                        has_symbol = true;
                    }
                }
                if let Some(neb_sym) = neb_symbol {
                    let symbol_type = neb_sym.symbol_type();
                    match symbol_type {
                        SymbolType::Transformer
                        | SymbolType::Partitioning
                        | SymbolType::Broadcasting
                        | SymbolType::Aggregation => {
                            let node_id = self.push_node(neb_sym, vec![]).id;
                            if prev_id > 0 {
                                self.link(prev_id, node_id);
                            }
                            // Recursively construst (potential) nested nodes
                            // Skip the first element for the symbol is included in the node
                            let params_opts = ele
                                .into_iter()
                                .skip(1)
                                .map(|pexpr| self.construct_from_expr(node_id, pexpr, id_counter))
                                .collect_vec();
                            let mut params = Vec::with_capacity(params_opts.capacity());
                            for popt in params_opts {
                                let construct_rt = popt?;
                                params.push(construct_rt);
                            }
                            let node = self.get_node_mut(node_id).unwrap();
                            node.params = params;
                            return Ok(Expr::META(Box::new(Expr::Value(OwnedValue::U32(node_id)))));
                        }
                        SymbolType::Operation => {
                            // Recursively construst (potential) nested nodes
                            let params_opts = ele
                                .into_iter()
                                .map(|pexpr| self.construct_from_expr(prev_id, pexpr, id_counter))
                                .collect_vec();
                            let mut eles = Vec::with_capacity(params_opts.len());
                            for popt in params_opts {
                                eles.push(popt?);
                            }
                            // Does not create a new node for each operations
                            // Instead, return the original expr list
                            return Ok(Expr::List(eles));
                        }
                        SymbolType::Macro => unreachable!(),
                    }
                } else if has_symbol {
                    // Other symbol, need to use loc-do
                    let params_opts = ele
                        .into_iter()
                        .map(|pexpr| self.construct_from_expr(prev_id, pexpr, id_counter))
                        .collect_vec();
                    let mut eles = Vec::with_capacity(params_opts.len() + 1);
                    // Rewrite the list into a new one prefix with local-do symbol
                    eles.push(Expr::Symbol(
                        neb_symbol_id(NebSymbol::LocalDo),
                        String::new(),
                    ));
                    for popt in params_opts {
                        eles.push(popt?);
                    }
                    // Return the original expr list with local-do as prefix
                    return Ok(Expr::List(eles));
                }
                return Ok(Expr::List(ele));
            }
            _ => return Ok(expr),
        }
    }

    pub fn static_type_check(&self, rev_topo_sorted: &Vec<u32>) -> Result<(), String> {
        for &node_id in rev_topo_sorted.iter() {
            let dependences = self.outlinks.get(&node_id);
            if let Some(node) = self.get_node(node_id) {
                let (in_tys, out_ty) = node.symbol.symbol_obj().io_types();
                let in_links = self.inlinks(node.id);
                if in_links.len() != in_tys.len() {
                    return Err(format!(
                        "Number of parameters does not match for {}, expecting {} but got {}",
                        node.symbol.symbol_name(),
                        in_tys.len(),
                        in_links.len()
                    ));
                }
                for in_ty in in_tys {}
            }
        }
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use dovahkiin::integrated::lisp::parse_to_serde_expr;
    use dovahkiin::parser::lisp::ParserExpr;
    use lightning::aarc::Arc;

    use super::*;

    #[test]
    fn test_add_nodes_and_edges() {
        let mut dag = DAG::new();
        let node1 = dag.push_node(NebSymbol::All, vec![]).id;
        let node2 = dag.push_node(NebSymbol::All, vec![]).id;
        let node3 = dag.push_node(NebSymbol::All, vec![]).id;

        dag.link(node1, node2);
        dag.link(node2, node3);

        assert_eq!(dag.nodes.len(), 3);
        assert_eq!(dag.outlinks[&node1], vec![node2]);
        assert_eq!(dag.inlinks[&node2], vec![node1]);
        assert_eq!(dag.outlinks[&node2], vec![node3]);
        assert_eq!(dag.inlinks[&node3], vec![node2]);
    }

    #[test]
    fn test_topological_sort() {
        let mut dag = DAG::new();
        let node1 = dag.push_node(NebSymbol::All, vec![]).id;
        let node2 = dag.push_node(NebSymbol::All, vec![]).id;
        let node3 = dag.push_node(NebSymbol::All, vec![]).id;
        let node4 = dag.push_node(NebSymbol::All, vec![]).id;
        let node5 = dag.push_node(NebSymbol::All, vec![]).id;
        let node6 = dag.push_node(NebSymbol::All, vec![]).id;

        dag.link(node1, node2);
        dag.link(node1, node3);
        dag.link(node2, node4);
        dag.link(node3, node4);
        dag.link(node4, node5);
        dag.link(node5, node6);

        // 1 -> 2 -> 4 -> 5 -> 6
        //   -> 3 --/^

        let topo_sorted = dag.rev_topological_sort();
        assert_eq!(topo_sorted, vec![6, 5, 4, 2, 3, 1]);
    }

    #[test]
    fn test_group_into_stages() {
        // In this case we will use a most typical cell query statement
        // (filter-shared-value
        //    (id-cell-sel
        //      (cell-id-query SCHEMA)
        //     FIELDS)
        //   FILTER)
        let mut dag = DAG::new();
        let node1 = dag.push_node(NebSymbol::CellIdQuery, vec![]).id;
        let node2 = dag.push_node(NebSymbol::IdCellSel, vec![]).id;
        let node3 = dag.push_node(NebSymbol::FilterSharedValue, vec![]).id;

        dag.link(node1, node2);
        dag.link(node2, node3);

        let topo_sorted = dag.rev_topological_sort();
        let stages = dag.group_into_stages(topo_sorted);

        // There should be 1 stages, cuz there is only one partition function
        assert_eq!(stages.len(), 1);

        assert_eq!(stages[0].len(), 1);
        assert_eq!(stages[0][0].nodes.len(), 3);
        assert_eq!(stages[0][0].nodes[0], 3);
        assert_eq!(stages[0][0].nodes[1], 2);
        assert_eq!(stages[0][0].nodes[2], 1);
    }

    #[test]
    fn test_construct_from_expr_single_stage() {
        let str_expr = "(filter-shared-value  (= 1u32 :a) (id-cell-sel (rev [:a :b :c]) (cell-id-query 1u32)))";
        let exprs = parse_to_serde_expr(str_expr).unwrap();
        let mut env = Environment::new(Arc::null());
        let dag = DAG::from_exprs(exprs, &mut env).unwrap();
        let topo_sorted = dag.rev_topological_sort();
        assert_eq!(topo_sorted, vec![3, 2, 1]);

        let stages = dag.group_into_stages(topo_sorted);

        assert_eq!(dag.nodes[0].symbol, NebSymbol::FilterSharedValue);
        assert_eq!(dag.nodes[1].symbol, NebSymbol::IdCellSel);
        assert_eq!(dag.nodes[2].symbol, NebSymbol::CellIdQuery);

        assert_eq!(dag.nodes[0].id, 1);
        assert_eq!(dag.nodes[1].id, 2);
        assert_eq!(dag.nodes[2].id, 3);

        assert_eq!(
            dag.nodes[0].params,
            vec![
                Expr::List(vec![
                    Expr::symbol("=".to_string()),
                    Expr::owned_val(OwnedValue::U32(1)),
                    Expr::keyword("a".to_string())
                ]),
                Expr::META(Box::new(Expr::Value(OwnedValue::U32(2))))
            ]
        );

        assert_eq!(
            dag.nodes[1].params,
            vec![
                Expr::List(vec![
                    Expr::Symbol(neb_symbol_id(NebSymbol::LocalDo), String::new()),
                    Expr::symbol("rev".to_string()),
                    Expr::Vec(vec![
                        Expr::keyword("a".to_string()),
                        Expr::keyword("b".to_string()),
                        Expr::keyword("c".to_string())
                    ])
                ]),
                Expr::META(Box::new(Expr::Value(OwnedValue::U32(3))))
            ]
        );

        assert_eq!(
            dag.nodes[2].params,
            vec![Expr::owned_val(OwnedValue::U32(1)),]
        );

        // There should be 1 stages, cuz there is only one partition function
        assert_eq!(stages.len(), 1);
        // The stage would have all transformers
        assert_eq!(stages[0].len(), 1);
        assert_eq!(stages[0][0].nodes.len(), 3);
        assert!(stages[0][0].partitioned);
        assert_eq!(stages[0][0].nodes, vec![3, 2, 1]);
    }

    #[test]
    fn test_construct_from_expr_mult_stage() {
        let str_expr = "(sort-by-asc (+ 3u32 data-field) (filter-shared-value  (= 2u32 :a) (id-cell-sel (rev [:a :b :c]) (cell-id-query 1u32))))";
        let exprs = parse_to_serde_expr(str_expr).unwrap();
        let mut env = Environment::new(Arc::null());
        let dag = DAG::from_exprs(exprs, &mut env).unwrap();
        let topo_sorted = dag.rev_topological_sort();
        assert_eq!(topo_sorted, vec![4, 3, 2, 1]);

        let stages = dag.group_into_stages(topo_sorted);

        assert_eq!(dag.nodes[0].symbol, NebSymbol::SortByASC);
        assert_eq!(dag.nodes[1].symbol, NebSymbol::FilterSharedValue);
        assert_eq!(dag.nodes[2].symbol, NebSymbol::IdCellSel);
        assert_eq!(dag.nodes[3].symbol, NebSymbol::CellIdQuery);

        assert_eq!(dag.nodes[0].id, 1);
        assert_eq!(dag.nodes[1].id, 2);
        assert_eq!(dag.nodes[2].id, 3);
        assert_eq!(dag.nodes[3].id, 4);

        assert_eq!(
            dag.nodes[0].params,
            vec![
                Expr::List(vec![
                    Expr::Symbol(neb_symbol_id(NebSymbol::LocalDo), String::new()),
                    Expr::symbol("+".to_string()),
                    Expr::owned_val(OwnedValue::U32(3)),
                    Expr::symbol("data-field".to_string())
                ]),
                Expr::META(Box::new(Expr::Value(OwnedValue::U32(2))))
            ]
        );

        assert_eq!(
            dag.nodes[1].params,
            vec![
                Expr::List(vec![
                    Expr::symbol("=".to_string()),
                    Expr::owned_val(OwnedValue::U32(2)),
                    Expr::keyword("a".to_string())
                ]),
                Expr::META(Box::new(Expr::Value(OwnedValue::U32(3))))
            ]
        );

        assert_eq!(
            dag.nodes[2].params,
            vec![
                Expr::List(vec![
                    Expr::Symbol(neb_symbol_id(NebSymbol::LocalDo), String::new()),
                    Expr::symbol("rev".to_string()),
                    Expr::Vec(vec![
                        Expr::keyword("a".to_string()),
                        Expr::keyword("b".to_string()),
                        Expr::keyword("c".to_string())
                    ])
                ]),
                Expr::META(Box::new(Expr::Value(OwnedValue::U32(4))))
            ]
        );

        assert_eq!(
            dag.nodes[3].params,
            vec![Expr::owned_val(OwnedValue::U32(1)),]
        );

        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].len(), 1);
        assert!(stages[0][0].partitioned);
        assert_eq!(stages[0][0].nodes, vec![4, 3, 2]);

        assert_eq!(stages[0].len(), 1);
        assert!(stages[1][0].partitioned);
        assert_eq!(stages[1][0].nodes, vec![1]);
    }
}

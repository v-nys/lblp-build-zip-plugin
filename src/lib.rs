use anyhow;
use base64::{engine::general_purpose::STANDARD as BASE64_ENGINE, Engine};
use extism_pdk::*;
use logic_based_learning_paths::domain_without_loading::{
    ArchivePayload, ArtifactMapping, EdgeType, FileReadBase64AnyClusterOperationInPayload,
    FileReadBase64OperationOutPayload, FileWriteBase64OperationInPayload, NodeID, ParamsSchema,
    RootedSupercluster, UnlockingCondition, WorkflowStepProcessingResult,
};
use logic_based_learning_paths::graph_analysis;
use petgraph::graph::NodeIndex;
use petgraph::visit::{EdgeRef, IntoNeighbors, IntoNodeReferences};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Write};
use zip::write::FileOptions;
use zip::CompressionMethod;
use zip::ZipWriter;

#[derive(Serialize)]
struct ReadableUnlockingCondition {
    pub all_of: HashSet<String>,
    pub one_of: HashSet<String>,
}

#[host_fn]
extern "ExtismHost" {
    fn write_binary_file_base64(payload: FileWriteBase64OperationInPayload) -> ();
    fn read_binary_file_base64_from_any_cluster(
        payload: FileReadBase64AnyClusterOperationInPayload,
    ) -> FileReadBase64OperationOutPayload;
}

#[plugin_fn]
pub fn get_params_schema(_: ()) -> FnResult<ParamsSchema> {
    Ok(ParamsSchema {
        schema: HashMap::new(),
    })
}

type ToBytesZipWriter = ZipWriter<Cursor<Vec<u8>>>;

fn add_yamlified_graph(
    supercluster: &RootedSupercluster,
    zip: &mut ToBytesZipWriter,
    options: FileOptions,
) -> anyhow::Result<()> {
    let (supercluster, roots) = (&supercluster.graph, &supercluster.roots);

    // graph without nodes would not be valid
    let mut serialized = "nodes:\n".to_string();
    supercluster.node_weights().for_each(|(id, title)| {
        serialized.push_str(&format!("  - id: {}\n", id));
        serialized.push_str(&format!("    title: {}\n", title));
    });

    // TODO: misschien gebruik maken van partition op edge_references?
    let all_type_edges: Vec<_> = supercluster
        .edge_references()
        .filter(|e| e.weight() == &EdgeType::All)
        .map(|e| {
            Option::zip(
                supercluster.node_weight(e.source()),
                supercluster.node_weight(e.target()),
            )
            .map(|(n1, n2)| (n1.0.clone(), n2.0.clone()))
        })
        .flatten()
        .collect();
    let any_type_edges: Vec<_> = supercluster
        .edge_references()
        .filter(|e| e.weight() == &EdgeType::AtLeastOne)
        .map(|e| {
            Option::zip(
                supercluster.node_weight(e.source()),
                supercluster.node_weight(e.target()),
            )
            .map(|(n1, n2)| (n1.0.clone(), n2.0.clone()))
        })
        .flatten()
        .collect();

    if all_type_edges.len() > 0 {
        serialized.push_str("all_type_edges:\n");
        all_type_edges.iter().for_each(|(id1, id2)| {
            serialized.push_str(&format!("  - start_id: {}\n", id1));
            serialized.push_str(&format!("    end_id: {}\n", id2));
        })
    }
    if any_type_edges.len() > 0 {
        serialized.push_str("any_type_edges:\n");
        any_type_edges.iter().for_each(|(id1, id2)| {
            serialized.push_str(&format!("  - start_id: {}\n", id1));
            serialized.push_str(&format!("    end_id: {}\n", id2));
        })
    }
    if roots.len() > 0 {
        serialized.push_str("roots:\n");
        roots.iter().for_each(|root| {
            serialized.push_str(&format!("  - {}\n", root));
        });
    }
    zip.start_file("serialized_complete_graph.yaml", options)?;
    zip.write(serialized.as_bytes())?;
    Ok(())
}

fn include_artifacts(
    archive_payload: &ArchivePayload,
    zip: &mut ToBytesZipWriter,
    options: FileOptions,
) -> anyhow::Result<()> {
    for ArtifactMapping {
        local_file,
        root_relative_target_dir,
    } in archive_payload.artifact_mapping.iter()
    {
        zip.start_file(
            root_relative_target_dir
                .join(
                    local_file
                        .file_name()
                        .expect("Artifacts should be files, not directories."),
                )
                .to_string_lossy(),
            options,
        )?;
        unsafe {
            let payload = read_binary_file_base64_from_any_cluster(
                FileReadBase64AnyClusterOperationInPayload {
                    absolute_path: local_file.to_string_lossy().into(),
                },
            )?;
            let base64_as_bytes = BASE64_ENGINE.decode(&payload.contents)?;
            zip.write_all(&base64_as_bytes)?;
        }
    }
    Ok(())
}

fn add_unlocking_conditions(
    supercluster: &RootedSupercluster,
    zip: &mut ToBytesZipWriter,
    options: FileOptions,
) -> anyhow::Result<()> {
    let (
        (
            dependent_to_dependency_graph,
            dependent_to_dependency_tc,
            dependent_to_dependency_revmap,
            dependent_to_dependency_toposort_order,
        ),
        (
            dependency_to_dependent_graph,
            dependency_to_dependent_tc,
            dependency_to_dependent_revmap,
            dependency_to_dependent_toposort_order,
        ),
        motivations_graph,
    ) = graph_analysis::dependency_helpers(&supercluster);
    let mut unlocking_conditions: HashMap<NodeID, Option<UnlockingCondition>> = HashMap::new();
    let roots = &supercluster.roots;
    supercluster.graph.node_references().for_each(
        |(_supercluster_node_index, (supercluster_node_id, _))| {
            if roots.contains(supercluster_node_id) {
                unlocking_conditions.insert(supercluster_node_id.clone(), None);
            } else {
                // dependent_to... uses a subgraph, so indexes are different!
                // matching_node = "all-type" graph counterpart to the current supercluster node
                let matching_nodes = dependency_to_dependent_graph
                    .node_references()
                    .filter(|(_idx, weight)| &weight.0 == supercluster_node_id)
                    .collect::<Vec<_>>();
                let matching_node = matching_nodes
                    .get(0)
                    .expect("Subgraph should contain all the supercluster nodes.");
                let matching_node_idx = matching_node.0.index();
                // denk dat dit strenger is dan nodig
                // dependent_to_dependency_tc betekent dat we *alle* harde dependencies zullen oplijsten
                // kan dit beperken tot enkel directe dependencies
                // i.e. de neighbors in dependent_to_depency_graph (neighbors = bereikbaar in één gerichte hop)
                let hard_dependency_ids: HashSet<NodeID> = dependent_to_dependency_tc
                    .neighbors(dependent_to_dependency_revmap[matching_node_idx])
                    .map(|ix: NodeIndex| dependent_to_dependency_toposort_order[ix.index()])
                    .filter_map(|idx| {
                        dependent_to_dependency_graph
                            .node_weight(idx)
                            .map(|(id, _)| id.clone())
                    })
                    .collect();
                let mut dependent_ids: HashSet<NodeID> = dependency_to_dependent_tc
                    .neighbors(dependency_to_dependent_revmap[matching_node.0.index()])
                    .map(|ix: NodeIndex| dependency_to_dependent_toposort_order[ix.index()])
                    .filter_map(|idx| {
                        dependency_to_dependent_graph
                            .node_weight(idx)
                            .map(|(id, _)| id.clone())
                    })
                    .collect();
                dependent_ids.insert(matching_node.1 .0.clone());
                let soft_dependency_ids = motivations_graph
                    .node_references()
                    .filter_map(|potential_motivator| {
                        let neighbors: HashSet<NodeID> = motivations_graph
                            .neighbors(potential_motivator.0)
                            .filter_map(|motivator_index| {
                                motivations_graph
                                    .node_weight(motivator_index)
                                    .map(|(id, _)| id.to_owned())
                            })
                            .collect();
                        if neighbors.is_disjoint(&dependent_ids) {
                            None
                        } else {
                            Some(potential_motivator.1 .0.to_owned())
                        }
                    })
                    .collect();
                unlocking_conditions.insert(
                    supercluster_node_id.clone(),
                    Some(UnlockingCondition {
                        all_of: hard_dependency_ids,
                        one_of: soft_dependency_ids,
                    }),
                );
            }
        },
    );
    let representation: HashMap<_, _> = unlocking_conditions
        .iter()
        .map(|(k, v)| {
            (
                format!("{}", k),
                v.as_ref().map(|condition| ReadableUnlockingCondition {
                    all_of: condition
                        .all_of
                        .iter()
                        .map(|node_id| format!("{}", node_id))
                        .collect(),
                    one_of: condition
                        .one_of
                        .iter()
                        .map(|node_id| format!("{}", node_id))
                        .collect(),
                }),
            )
        })
        .collect();
    zip.start_file("unlocking_conditions.json", options)?;
    zip.write(
        serde_json::to_string_pretty(&representation)
            .unwrap()
            .as_bytes(),
    )?;
    Ok(())
}

#[plugin_fn]
pub fn process_paths(archive_payload: ArchivePayload) -> FnResult<WorkflowStepProcessingResult> {
    let buffer: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(vec![]);
    let mut zip = zip::ZipWriter::new(buffer);
    let supercluster: &RootedSupercluster = &archive_payload.rooted_supercluster;
    let options = FileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o755);

    let yamlification_result = add_yamlified_graph(supercluster, &mut zip, options);
    let artifact_inclusion_result = include_artifacts(&archive_payload, &mut zip, options);
    let unlocking_conditions_result = add_unlocking_conditions(supercluster, &mut zip, options);

    let resulting_file = zip.finish();
    match resulting_file {
        Ok(cursor) => {
            let base64_text = BASE64_ENGINE.encode(cursor.into_inner());
            unsafe {
                let write_result = write_binary_file_base64(FileWriteBase64OperationInPayload {
                    relative_path: "archive.zip".to_owned(),
                    base64_text: base64_text,
                });
                write_result
                    .map(|_| WorkflowStepProcessingResult {
                        hash_set: archive_payload.artifact_mapping,
                    })
                    .map_err(|e| extism_pdk::WithReturnCode(e, 1))
            }
        }
        Err(_) => Err(extism_pdk::WithReturnCode(
            extism_pdk::Error::msg("Failed to write zip file."),
            1,
        )),
    }
}

use base64::{engine::general_purpose::STANDARD as BASE64_ENGINE, Engine};
use extism_pdk::*;
use logic_based_learning_paths::domain_without_loading::{
    ArchivePayload, EdgeType, FileWriteBase64OperationInPayload, ParamsSchema, RootedSupercluster,
    WorkflowStepProcessingResult,
};
use petgraph::visit::EdgeRef;
use std::collections::HashMap;
use std::io::Write;
use zip::write::FileOptions;
use zip::CompressionMethod;

#[host_fn]
extern "ExtismHost" {
    fn write_binary_file_base64(payload: FileWriteBase64OperationInPayload) -> ();

    // write required host functions here
    // fn get_last_modification_time(relative_path: String) -> SystemTimePayload;
    // will need something to read a file as bytes, probably
}

#[plugin_fn]
pub fn get_params_schema(_: ()) -> FnResult<ParamsSchema> {
    Ok(ParamsSchema {
        schema: HashMap::new(),
    })
}

#[plugin_fn]
pub fn process_paths(archive_payload: ArchivePayload) -> FnResult<WorkflowStepProcessingResult> {
    // Ok(WorkflowStepProcessingResult {
    //     hash_set: archive_payload.artifact_mapping,
    // })
    // let zip_path = std::path::Path::new("archive.zip");
    let buffer: std::io::Cursor<Vec<u8>> = std::io::Cursor::new(vec![]);
    let mut zip = zip::ZipWriter::new(buffer);
    let supercluster: RootedSupercluster = archive_payload.rooted_supercluster;
    let options = FileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o755);
    {
        let (supercluster, roots) = (&supercluster.graph, &supercluster.roots);

        // graph without nodes would not be valid
        let mut serialized = "nodes:\n".to_string();
        supercluster.node_weights().for_each(|(id, title)| {
            serialized.push_str(&format!("  - id: {}\n", id));
            serialized.push_str(&format!("    title: {}\n", title));
        });
        // misschien gebruik maken van partition op edge_references?
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

        let _ = zip.start_file("serialized_complete_graph.yaml", options); // TODO: use result
        let _ = zip.write(serialized.as_bytes()); // ditto
    }
    // TODO: add stuff from before to zip
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
        Err(e) => Err(extism_pdk::WithReturnCode(
            extism_pdk::Error::msg("Failed to write zip file."),
            1,
        )),
    }
}

use extism_pdk::*;
use logic_based_learning_paths::domain_without_loading::{
    ArchivePayload, ArtifactMapping, EdgeType, ParamsSchema,
};
use std::collections::HashMap;
use std::collections::HashSet;
use zip::write::FileOptions;
use zip::CompressionMethod;

#[host_fn]
extern "ExtismHost" {
    // write required host functions here
    // fn get_last_modification_time(relative_path: String) -> SystemTimePayload;
    // will need something to read a file as bytes, probably
}

#[plugin_fn]
pub fn get_params_schema(_: ()) -> FnResult<ParamsSchema> {
    let parameters = HashMap::new();
    // insert key-value pairs into `parameters` here
    Ok(ParamsSchema {
        schema: HashMap::new(),
    })
}

#[plugin_fn]
pub fn process_paths(_: ArchivePayload) -> FnResult<()> {
    let zip_path = std::path::Path::new("archive.zip");
    // may need a host function for this?
    // or may be able to create everything locally and then copy
    let zip_file = std::fs::File::create(zip_path).map_err(|e| e.to_string())?;
    // copy clusters into zipped folder
    let mut zip = zip::ZipWriter::new(zip_file);

    let supercluster: RootedSuperCluster =
        todo!("Need to supply this, used to just grab the app state.");
    let _component_clusters_and_artifact_mappings: Vec<(
        domain::Cluster,
        HashSet<ArtifactMapping>,
    )> = todo!("need to supply this");

    let options = FileOptions::default()
        .compression_method(CompressionMethod::Stored)
        .unix_permissions(0o755);
    // TODO: factor this out?
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
        let _ = zip.write(serialized.as_bytes()); // same
    }

    // TODO: add more stuff from before

    zip.finish()
        .map(|_| zip_path.to_path_buf())
        .map_err(|ze| ze.to_string())
}

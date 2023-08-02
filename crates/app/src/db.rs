use idb::{Database, Error, Factory, ObjectStoreParams, KeyPath, IndexParams};

async fn create_database() -> Result<Database, Error> {
    // Get a factory instance from global scope
    let factory = Factory::new()?;

    // Create an open request for the database
    let mut open_request = factory.open("hyva_kartta", Some(1)).unwrap();

    // Add an upgrade handler for database
    open_request.on_upgrade_needed(|event| {
        // Get database instance from event
        let database = event.database().unwrap();

        // Prepare object store params
        let mut store_params = ObjectStoreParams::new();
        store_params.auto_increment(true);
        store_params.key_path(Some(KeyPath::new_single("id")));

        // Create object store
        let store = database
            .create_object_store("employees", store_params)
            .unwrap();

        // Prepare index params
        let mut index_params = IndexParams::new();
        index_params.unique(true);

        // Create index on object store
        store
            .create_index("email", KeyPath::new_single("email"), Some(index_params))
            .unwrap();
    });

    // `await` open request
    open_request.await
}
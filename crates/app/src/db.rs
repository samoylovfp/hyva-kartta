use idb::{Database, Error, Factory, IndexParams, KeyPath, ObjectStoreParams};

pub async fn create_database() -> Result<Database, Error> {
    // Get a factory instance from global scope
    let factory = Factory::new()?;

    // Create an open request for the database
    let mut open_request = factory.open("hyva_kartta", Some(1)).unwrap();

    // Add an upgrade handler for database
    open_request.on_upgrade_needed(|event| {
        // Get database instance from event
        let database = event.database().unwrap();

        // Prepare object store params
        let store_params = ObjectStoreParams::new();

        // Create object store
        let store = database.create_object_store("cells", store_params).unwrap();
    });

    // `await` open request
    open_request.await
}

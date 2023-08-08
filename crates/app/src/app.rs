use std::{cell::Cell, collections::HashMap, sync::Arc};

use osmpbfreader::{OsmObj, OsmPbfReader};
use wasm_bindgen_futures::spawn_local;

module walrus::wdynamic_field;

use sui::dynamic_field as df;
use std::debug;

public fun add<Name: copy + drop + store, Value: store>(
    // we use &mut UID in several spots for access control
    object: &mut UID,
    name: Name,
    value: Value,
) {
    debug::print(&111);
    df::add(object, name, value)
}

public fun borrow<Name: copy + drop + store, Value: store>(object: &UID, name: Name): &Value {
    debug::print(&222);
    df::borrow(object, name)
}

public fun borrow_mut<Name: copy + drop + store, Value: store>(
    object: &mut UID,
    name: Name,
): &mut Value {
    debug::print(&333);
    df::borrow_mut(object, name)
}

public fun remove<Name: copy + drop + store, Value: store>(object: &mut UID, name: Name): Value {
    debug::print(&444);
    df::remove(object, name)
}

public fun exists_<Name: copy + drop + store>(object: &UID, name: Name): bool {
    debug::print(&555);
    df::exists_(object, name)
}

public fun remove_if_exists<Name: copy + drop + store, Value: store>(
    object: &mut UID,
    name: Name,
): Option<Value> {
    debug::print(&666);
    df::remove_if_exists(object, name)
}

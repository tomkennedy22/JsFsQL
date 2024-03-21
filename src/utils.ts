import { type_partition_index } from "./types";
import * as dayjs from 'dayjs'

// Helper function to generate a partition name based on the partition index.
// It converts the index into a string format suitable for file naming.
export const partition_name_from_partition_index = (partition_index: type_partition_index): string => {
    return Object.entries(partition_index)
        .map(([indexKey, indexValue]) => `${indexKey}_${indexValue}`)
        .join('_') || 'default';
}

export const distinct = (arr: any[]): any[] => {
    return arr.reduce((acc: any[], current: any) => {
        if (!acc.some(item => is_deep_equal(item, current))) {
            acc.push(current);
        }
        return acc;
    }, []);
};


export const get_from_dict = (obj: object, key: string): any => {
    let keyParts: string[] = String(key).split('.');;
    let current: any = obj;

    for (let i = 0; i < keyParts.length; i++) {
        const part = keyParts[i];

        // Check if current is a Map and get the value
        if (current instanceof Map) {
            if (!current.has(part)) {
                return null; // Key not found in Map
            }
            current = current.get(part);
        }
        // Check if current is an object and get the value
        else if (typeof current === 'object' && current !== null) {
            if (!(part in current)) {
                return null; // Key not found in object
            }
            current = current[part];
        }
        // If current is neither an object nor a Map
        else {
            return null;
        }
    }

    return current;
}


export const set_to_dict = (container: { [key: string]: any } | Map<any, any>, key: string, value: any) => {
    key = `${key}`.trim();
    const keys = key.split('.');
    let current_container = container;

    for (let i = 0; i < keys.length; i++) {
        const current_key = keys[i];

        if (i === keys.length - 1) {
            if (current_container instanceof Map) {
                current_container.set(current_key, value);
            } else {
                (current_container as { [key: string]: any })[current_key] = value;
            }
        } else {
            const next_key = keys[i + 1];
            if (current_container instanceof Map) {
                if (!current_container.has(current_key) || !(current_container.get(current_key) instanceof Map)) {
                    current_container.set(current_key, new Map<any, any>());
                }
                current_container = current_container.get(current_key);
            } else {
                if (!current_container[current_key] || typeof current_container[current_key] !== 'object') {
                    (current_container as { [key: string]: any })[current_key] = {};
                }
                current_container = current_container[current_key];
            }
        }
    }

    return container;
};



export const deep_copy = (obj: any, hash = new WeakMap()): any => {
    // Handle null, undefined, and non-objects
    if (obj === null || typeof obj !== 'object') {
        return obj;
    }

    // console.log('deep_copy', {obj, constructor: obj.constructor});

    // Handle Date
    if (obj instanceof Date) {
        return new Date(obj);
    }

    // Handle Array
    if (Array.isArray(obj)) {
        return obj.map(item => deep_copy(item, hash));
    }

    // Handle objects already copied
    if (hash.has(obj)) {
        return hash.get(obj);
    }

    // Handle Object and class instances
    // const result = new obj.constructor();
    const result = Object.create(obj.constructor.prototype);
    hash.set(obj, result);

    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            result[key] = deep_copy(obj[key], hash);
        }
    }

    return result;
}


export const is_deep_equal = (obj1: any, obj2: any) => {
    if (obj1 === obj2) {
        return true;
    }

    if (typeof obj1 !== 'object' || typeof obj2 !== 'object' || obj1 == null || obj2 == null) {
        return false;
    }

    const keys1 = Object.keys(obj1);
    const keys2 = Object.keys(obj2);

    if (keys1.length !== keys2.length) {
        return false;
    }

    for (const key of keys1) {
        if (!keys2.includes(key) || !is_deep_equal(obj1[key], obj2[key])) {
            return false;
        }
    }

    return true;
};

export const nest_children = (parent_array: any, child_dict: { [x: string]: any; }, join_key: string, store_key: string) => {

    for (const parent of parent_array) {
        parent[store_key] = child_dict[parent[join_key]];
    }

    return parent_array;
};

export const index_by = (list: any[], index_field: string) => {
    // console.log('index_by', { list, index_field });
    let index_map: any = {};
    for (let row of list) {
        let index_value = get_from_dict(row, index_field);
        index_map[index_value] = row;
    }

    return index_map;
}

export const group_by = (list: any[], index_field: string) => {
    let group_map: any = {};
    for (let row of list) {
        let index_value = get_from_dict(row, index_field);
        if (!(group_map[index_value])) {
            group_map[index_value] = [];
        }
        group_map[index_value].push(row);
    }

    return group_map;
}

export const first_element = (list: any[]): any => {
    if (list.length === 0) {
        return null;
    }
    return list[0];
}

export const yield_nested_children = (parent_objects: any[] | any, search_key: string | string[], found_children: any[] = []): any[] => {
    let parents = Array.isArray(parent_objects) ? parent_objects : [parent_objects];
    parents = parents.filter(parent => parent !== null && typeof parent === 'object');

    let search_key_list = Array.isArray(search_key) ? search_key : [search_key];

    for (let parent_object of parents) {
        for (let search_key_name of search_key_list) {
            if (parent_object[search_key_name]) {
                if (Array.isArray(parent_object[search_key_name])) {
                    found_children.push(...parent_object[search_key_name]);
                } else {
                    found_children.push(parent_object[search_key_name]);
                }
            }

            for (let key in parent_object) {
                if (parent_object.hasOwnProperty(key) && typeof parent_object[key] === 'object') {
                    yield_nested_children(parent_object[key], search_key_name, found_children);
                }
            }
        }
    }

    return found_children;
}


export const print_nested_object = (obj: any, indent = 0): void => {
    for (const key in obj) {
        if (typeof obj[key] === 'object' && obj[key] !== null) {
            console.log(' '.repeat(indent) + key + ':');
            print_nested_object(obj[key], indent + 2); // Increase indentation for nested objects
        } else {
            console.log(' '.repeat(indent) + key + ': ' + obj[key]);
        }
    }
}

export const flatten_dict = (obj: any, prefix = ''): any => {
    return Object.keys(obj).reduce<any>((accumulator, key) => {
        const prefixedKey = prefix ? `${prefix}.${key}` : key;
        if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
            Object.assign(accumulator, flatten_dict(obj[key], prefixedKey));
        } else {
            accumulator[prefixedKey] = obj[key];
        }
        return accumulator;
    }, {});
}
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
    return [...new Set(arr)];
};

export const get_from_dict = (obj: object | Map<any, any>, key: string): any => {
    const keyParts = key.split('.');
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



export const set_to_dict = (obj: any, key: string, value: any) => {
    key = `${key}`.trim();
    const keys = key.split('.');
    let current_obj = obj;

    for (let i = 0; i < keys.length; i++) {
        const current_key = keys[i];

        if (i === keys.length - 1) {
            current_obj[current_key] = value;
        } else {
            current_obj[current_key] = current_obj[current_key] || {};
        }

        current_obj = current_obj[current_key];
    }

    return obj;
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

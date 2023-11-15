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


export const get_from_dict = (obj: any, key: string) => {
    key = `${key}`.trim();

    let key_parts = key.split(".");
    let iter_obj = obj;
    let loop_count = 0;
    let max_loop = key_parts.length;
    if (max_loop == 1){
        return obj[key];
    }
    // else if (max_loop > 1) {
    //     console.log('get_from_dict', {key_parts, key, iter_obj, loop_count, max_loop})
    // }
    for (let key_part of key_parts) {
        loop_count += 1;
        if (loop_count == max_loop) {
            if (key_part in iter_obj) {
                return iter_obj[key_part];
            }
            return null;
        }
        if (typeof iter_obj === "object") {
            if (iter_obj[key_part]) {
                iter_obj = iter_obj[key_part];
                continue;
            } else {
                return null;
            }
        }
    }
};

export const set = (obj: any, key: string, value: any) => {
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

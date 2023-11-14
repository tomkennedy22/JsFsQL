import { type_partition_index } from "./types";

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
    if (typeof key !== 'string') {
        console.error(`Error in get_from_dict. Expected a string, got ${typeof key} for variable "key"`);
        return null;
    }

    let key_parts = key.split(".");
    let iter_obj = obj;
    let loop_count = 0;
    let max_loop = key_parts.length;
    for (let key_part of key_parts) {
        loop_count += 1;
        if (loop_count == max_loop) {
            if (key_part in iter_obj) {
                return iter_obj[key_part];
            }
            return null;
        }
        if (typeof iter_obj === "object") {
            if (key_part in iter_obj) {
                iter_obj = iter_obj[key_part];
                continue;
            } else {
                return null;
            }
        }
    }
};

export const set = (obj: any, key: string, value: any) => {
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

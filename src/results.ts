import { table } from "./table";
import { type_query, type_results } from "./types";
import { deep_copy, distinct, get_from_dict, set_to_dict } from "./utils";
import { stringify, parse } from 'flatted';

export class results<T extends object> extends Array implements type_results {

    constructor(elements?: T[] | T) {
        if (elements && !Array.isArray(elements)) {
            elements = [elements];
        }
        else if (!elements) {
            elements = [];
        }

        const serialized = stringify(elements);
        const deserialized = parse(serialized);
        super(...deserialized);
    }

    make_copy() {
        return new results(this.map(row => deep_copy(row)));
    }

    index_by(index_field: string) {
        let index = {};
        for (let row of this) {
            let index_value = get_from_dict(row, index_field);
            // set_to_dict(index, index_value, deep_copy(row));
            set_to_dict(index, index_value, row);
        }

        return index;
    }

    group_by(group_by_field: string): { [key: string]: any[] } {
        let groups = {};
        for (let row of this) {
            let group_by_value = get_from_dict(row, group_by_field);

            if (!get_from_dict(groups, group_by_value)) {
                set_to_dict(groups, group_by_value, [])
            }
            let group = get_from_dict(groups, group_by_value);
            group.push(row);
        }

        return groups;
    }


    first(): T | null{
        if (this.length == 0) {
            return null;
        }

        return this[0];
    }

}
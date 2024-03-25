import { type_database, type_loose_query, type_results } from "./types";
import { distinct, get_from_dict, index_by, group_by, nest_children, deep_copy, first_element, print_nested_object } from "./utils";


export class QueryNode {

    name: string;
    alias: string;
    filter: type_loose_query;
    sort?: any;
    find_fn?: 'find' | 'findOne';
    filter_up?: boolean;
    children_obj?: { [key: string]: type_join_criteria };
    children_list: QueryNode[];
    parent_node?: QueryNode;

    results: type_results = [];
    found_ids?: any[] = [];

    constructor(name: string, init_data: type_join_criteria) {
        this.filter = init_data.filter || {};
        this.sort = init_data.sort || {};
        this.find_fn = init_data.find_fn || 'find';
        this.name = name || '';
        this.alias = init_data.alias || name;
        this.filter_up = init_data.filter_up || false;
        this.children_obj = init_data.children || {};
        this.children_list = [];
    }

    join(db: type_database): type_results {

        let table = db.tables[this.name];
        this.found_ids = []

        let find_fn: 'find' | 'findOne' = this.find_fn || 'find';
        let name = this.name;

        let parent_connection: any;
        let parent_join_key: string | null = null;
        let parent_values;

        // If there is a parent, get the join key, then find all distinct values. 
        // This speeds up the FIND function below by adding more specificity to the filter.
        if (this.parent_node) {
            parent_connection = table.table_connections[this.parent_node.name];
            parent_join_key = parent_connection.join_key;
            if (parent_join_key) {
                parent_values = distinct(this.parent_node.results.map(row => get_from_dict(row, parent_join_key as string))).filter(value => value);

                if (parent_values.length > 0 && !this.filter[parent_join_key]) {
                    this.filter = {
                        ...this.filter,
                        [parent_join_key]: { $in: parent_values },
                    }
                    // this.filter[parent_join_key] = { $in: parent_values };
                }
            }
        }


        if (this.found_ids.length > 0) {
            // this.filter[table.primary_key] = { $in: this.found_ids };
            this.filter = {
                ...this.filter,
                [table.primary_key]: { $in: this.found_ids },
            }
        }
        this.results = table.find(this.filter);
        let results_by_join_key;


        // Run joins for all children, recursively
        for (let child of this.children_list) {
            this.results = child.join(db);
        }

        // Store found_ids for later use
        this.found_ids = this.results.map(row => get_from_dict(row, table.primary_key)).filter(value => value);

        // Set results_by_join_key based on join_type & parent_join_key
        if (this.parent_node && parent_connection && parent_join_key) {
            if (parent_connection.join_type === 'one_to_many' || parent_connection.join_type === 'one_to_one' || find_fn == 'findOne') {
                results_by_join_key = index_by(this.results, parent_join_key);
            }
            else if (parent_connection.join_type === 'many_to_one') {
                results_by_join_key = group_by(this.results, parent_join_key);
            }
        }

        // Sort results_by_join_key based on sort criteria
        if (this.parent_node && parent_connection && parent_join_key && (parent_connection.join_type === 'many_to_one' && !(this.find_fn == 'findOne')) && this.sort) {
            for (let key in results_by_join_key) {
                results_by_join_key[key] = results_by_join_key[key].sort((a: any, b: any) => {
                    for (let sort_key in this.sort) {
                        let sort_val = this.sort[sort_key];
                        let val_a = get_from_dict(a, sort_key);
                        let val_b = get_from_dict(b, sort_key);
                        if (val_a < val_b) {
                            return sort_val;
                        }
                        else if (val_a > val_b) {
                            return sort_val * -1;
                        }
                    }
                    return 0;
                });
            }
        }

        // Nest children into parent results
        if (this.parent_node && parent_connection && parent_join_key) {
            this.parent_node.results = nest_children(this.parent_node.results, results_by_join_key, parent_join_key, this.alias);
            // if (this.filter_up) {
            //     this.parent_node.results = this.parent_node.results.filter(row => row[name] != undefined);
            // }

            return this.parent_node.results;
        }
        else {
            return find_fn == 'find' ? this.results : first_element(this.results);
        }
    }

}

export class QueryGraph {

    root: QueryNode;
    graph_stats?: {
        most_specific_filter_node: QueryNode;
        depth_from_root: number;
    }

    constructor(init_data: { [key: string]: type_join_criteria }) {
        let root_key = Object.keys(init_data)[0];
        this.root = new QueryNode(root_key, { ...init_data[root_key] });

        this.build_graph(this.root);
        this.build_graph_stats();
    }

    build_graph_stats() {

        let most_specific_filter_count = this.root.filter ? Object.keys(this.root.filter).length : 0;
        let most_specific_filter_node: QueryNode | null = null;
        let depth_from_root = 0;

        let children_list: QueryNode[] = [this.root];

        while (children_list.length > 0) {
            let child = children_list.shift() as QueryNode;

            if (child.filter && Object.keys(child.filter).length > 0) {
                if (!most_specific_filter_node || Object.keys(child.filter).length > most_specific_filter_count) {
                    most_specific_filter_count = Object.keys(child.filter).length;
                    most_specific_filter_node = child;
                }
            }

            children_list.push(...child.children_list);
        }

        this.graph_stats = {
            most_specific_filter_node: most_specific_filter_node as QueryNode,
            depth_from_root: depth_from_root,
        }
    }

    node_as_join_criteria(node: QueryNode): type_join_criteria {

        let children: { [key: string]: type_join_criteria } = {};

        for (let child of node.children_list) {
            children[child.name] = this.node_as_join_criteria(child);
        }

        return {
            filter: node.filter,
            sort: node.sort,
            find_fn: node.find_fn,
            name: node.name,
            alias: node.alias,
            filter_up: node.filter_up,
            children: children,
        }

    }

    build_graph(root: QueryNode) {
        let children_obj = root.children_obj;

        for (let key in children_obj) {
            let child = children_obj[key];

            let node = new QueryNode(key, { ...child });
            node.parent_node = root;
            root.children_list.push(node);

            if (child.children) {
                this.build_graph(node);
            }
        }
    }

    reroot_from_node_key(new_root_key: string): void {
        let children_list: QueryNode[] = [...this.root.children_list];
        let new_root: QueryNode | null = null;

        let loop_count = 0;
        while (children_list.length > 0 && !new_root) {
            let child: QueryNode = children_list.shift() as QueryNode;
            if (child.name === new_root_key) {
                new_root = child;
                break;
            }
            children_list.push(...child.children_list);

            loop_count += 1;
        }

        if (new_root) {
            this.flip_parent_to_child(new_root, undefined);
            this.root = new_root;
        }

        this.build_graph_stats();

    }

    flip_parent_to_child(node: QueryNode, new_parent_node: QueryNode | undefined): void {
        let parent_node = node.parent_node;
        if (!parent_node) {
            node.parent_node = new_parent_node;
            return;
        }

        let parent_children = parent_node.children_list;

        if (parent_node.children_obj && node.name in parent_node.children_obj) {
            delete parent_node.children_obj[node.name];
        }

        let node_index = parent_children.indexOf(node);
        parent_children.splice(node_index, 1);

        node.children_list.push(parent_node);

        if (!node.children_obj) {
            node.children_obj = {};
        }
        node.children_obj[parent_node.name] = this.node_as_join_criteria(parent_node);
        node.parent_node = new_parent_node;

        this.flip_parent_to_child(parent_node, node);
    }


    reroot_from_most_filtered_node(): void {

        let most_specific_filter_node = this.graph_stats?.most_specific_filter_node;
        if (most_specific_filter_node) {
            this.reroot_from_node_key(most_specific_filter_node.name);
        }
    }


    orphan_node(node?: QueryNode): void {

        if (!node) {
            node = this.root;
        }

        for (let child of node.children_list) {
            this.orphan_node(child);
        }

        node.parent_node = undefined;
    }
}



type table_join_results = {
    data: any[];
    indexes: { [key: string]: { [key: string]: any } };
    groups: { [key: string]: { [key: string]: any[] } };
};

type join_results = {
    results: any[];
    tables: { [key: string]: table_join_results };
}

export type type_find_fn = 'find' | 'findOne';
export const constant_find_fn: { find: type_find_fn; findOne: type_find_fn } = { find: 'find', findOne: 'findOne' }

type type_join_criteria = {
    name?: string;
    filter?: type_loose_query;
    sort?: any;
    find_fn?: type_find_fn;
    alias?: string;
    filter_up?: boolean;
    children?: { [key: string]: type_join_criteria };
}

export const nested_join = (db: type_database, join_criteria: { [key: string]: type_join_criteria }): type_results => {
    let query_graph = new QueryGraph(join_criteria);
    let original_root_node_name = query_graph.root.name;
    query_graph.reroot_from_most_filtered_node();
    query_graph.root.join(db);

    query_graph.reroot_from_node_key(original_root_node_name);

    let results = query_graph.root.join(db);
    // query_graph.orphan_node();

    return results;
}
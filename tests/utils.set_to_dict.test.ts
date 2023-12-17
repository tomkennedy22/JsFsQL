import { set_to_dict } from '../src/utils';

describe('set_to_dict function tests', () => {

    test('sets simple key-value pair in an object', () => {
        const obj = {};
        set_to_dict(obj, 'key', 'value');
        expect(obj).toEqual({ key: 'value' });
    });

    test('updates existing key-value pair', () => {
        const obj = { key: 'oldValue' };
        set_to_dict(obj, 'key', 'newValue');
        expect(obj).toEqual({ key: 'newValue' });
    });

    test('sets nested key-value pair in an object', () => {
        const obj = {};
        set_to_dict(obj, 'a.b.c', 'value');
        expect(obj).toEqual({ a: { b: { c: 'value' } } });
    });

    test('updates value in nested key-value pair', () => {
        const obj = { a: { b: { c: 'oldValue' } } };
        set_to_dict(obj, 'a.b.c', 'newValue');
        expect(obj).toEqual({ a: { b: { c: 'newValue' } } });
    });

    test('does not overwrite existing nested objects', () => {
        const obj = { a: { b: { existing: 'value' } } };
        set_to_dict(obj, 'a.b.new', 'newValue');
        expect(obj).toEqual({ a: { b: { existing: 'value', new: 'newValue' } } });
    });

    test('handles non-existent nested paths', () => {
        const obj = {};
        set_to_dict(obj, 'x.y', 'value');
        expect(obj).toEqual({ x: { y: 'value' } });
    });

    test('sets value with leading and trailing spaces in key', () => {
        const obj = {};
        set_to_dict(obj, '  key  ', 'value');
        expect(obj).toEqual({ key: 'value' });
    });

    test('sets value with complex path', () => {
        const obj = {};
        set_to_dict(obj, 'part1.part2.part3', 'value');
        expect(obj).toEqual({ part1: { part2: { part3: 'value' } } });
    });

    test('handles empty key', () => {
        const obj = { existing: 'value' };
        set_to_dict(obj, '', 'newValue');
        expect(obj).toEqual({ existing: 'value', '': 'newValue' });
    });

    test('handles null object', () => {
        const obj = null;
        // @ts-ignore
        expect(() => set_to_dict(obj, 'key', 'value')).toThrow();
    });

    test('handles non-object types', () => {
        const obj = 123;
        // @ts-ignore
        expect(() => set_to_dict(obj, 'key', 'value')).toThrow();
    });

    test('sets value in a Map object', () => {
        const obj = new Map();
        set_to_dict(obj, 'key', 'value');
        expect(obj.get('key')).toBe('value');
    });

});

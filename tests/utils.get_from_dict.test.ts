import ts from 'typescript';
import { get_from_dict } from '../src/utils';

describe('get_from_dict function tests', () => {

    test('retrieves simple key from object', () => {
        expect(get_from_dict({ a: 1 }, 'a')).toBe(1);
    });

    test('retrieves simple key from Map', () => {
        const map = new Map([['a', 1]]);
        expect(get_from_dict(map, 'a')).toBe(1);
    });

    test('returns null for non-existent key', () => {
        expect(get_from_dict({ a: 1 }, 'b')).toBeNull();
    });

    test('handles nested keys in object', () => {
        expect(get_from_dict({ a: { b: { c: 2 } } }, 'a.b.c')).toBe(2);
    });

    test('handles nested keys with Map and object combination', () => {
        const map = new Map([['a', { b: 2 }]]);
        expect(get_from_dict(map, 'a.b')).toBe(2);
    });

    test('returns null for broken key path', () => {
        expect(get_from_dict({ a: { b: 2 } }, 'a.c')).toBeNull();
    });

    test('handles non-string keys gracefully', () => {
        // @ts-ignore
        expect(get_from_dict({ a: 1 }, 123)).toBeNull();
    });

    test('returns null for null object', () => {
        // @ts-ignore
        expect(get_from_dict(null, 'a')).toBeNull();
    });

    test('returns null for non-object and non-map', () => {
        // @ts-ignore
        expect(get_from_dict(123, 'a')).toBeNull();
    });

    test('handles arrays in object paths', () => {
        expect(get_from_dict({ a: [1, 2, 3] }, 'a.1')).toBe(2);
    });

});

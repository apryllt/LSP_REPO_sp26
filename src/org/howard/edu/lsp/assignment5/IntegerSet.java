package org.howard.edu.lsp.assignment5;

import java.util.ArrayList;
import java.util.Collections;

/**
 * A class that models a mathematical set of integers using an ArrayList.
 * This set does not allow duplicate values and supports standard set operations.
 */
public class IntegerSet {
     /** Internal storage for the set */
    private ArrayList<Integer> set;

    /**
     * Constructs an empty IntegerSet.
     */
    public IntegerSet() {
        set = new ArrayList<>();
    }

    /**
     * Removes all elements from the set.
     */
    public void clear() {
        set.clear();
    }

    /**
     * Returns the number of elements in the set.
     *
     * @return the size of the set
     */
    public int length() {
        return set.size();
    }

    /**
     * Compares this set with another set for equality.
     * Two sets are equal if they contain exactly the same elements.
     *
     * @param b the other IntegerSet to compare with
     * @return true if both sets contain the same elements, false otherwise
     */
    public boolean equals(IntegerSet b) {
        if (b == null) return false;
        if (this.length() != b.length()) return false;

        for (Integer val : set) {
            if (!b.contains(val)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether the set contains a given value.
     *
     * @param value the value to check
     * @return true if the value exists in the set, false otherwise
     */
    public boolean contains(int value) {
        return set.contains(value);
    }

    /**
     * Returns the largest value in the set.
     *
     * @return the maximum integer in the set
     * @throws RuntimeException if the set is empty
     */
    public int largest() {
        if (isEmpty()) {
            throw new RuntimeException("Set is empty");
        }
        return Collections.max(set);
    }

    /**
     * Returns the smallest value in the set.
     *
     * @return the minimum integer in the set
     * @throws RuntimeException if the set is empty
     */
    public int smallest() {
        if (isEmpty()) {
            throw new RuntimeException("Set is empty");
        }
        return Collections.min(set);
    }

    /**
     * Adds an item to the set if it is not already present.
     *
     * @param item the integer to add
     */
    public void add(int item) {
        if (!set.contains(item)) {
            set.add(item);
        }
    }

    /**
     * Removes an item from the set if it exists.
     *
     * @param item the integer to remove
     */
    public void remove(int item) {
        set.remove(Integer.valueOf(item));
    }

    /**
     * Returns a new set that is the union of this set and another set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing all unique elements from both sets
     */
    public IntegerSet union(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();

        // Add all elements from this set
        for (Integer val : this.set) {
            result.add(val);
        }

        // Add elements from b (duplicates automatically ignored)
        for (Integer val : intSetb.set) {
            result.add(val);
        }

        return result;
    }

    /**
     * Returns a new set that is the intersection of this set and another set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing common elements
     */
    public IntegerSet intersect(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();

        for (Integer val : this.set) {
            if (intSetb.contains(val)) {
                result.add(val);
            }
        }

        return result;
    }

    /**
     * Returns a new set that is the difference of this set and another set (this - b).
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing elements in this set but not in b
     */
    public IntegerSet diff(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();

        for (Integer val : this.set) {
            if (!intSetb.contains(val)) {
                result.add(val);
            }
        }

        return result;
    }

    /**
     * Returns the complement of this set with respect to another set (b - this).
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet containing elements in b but not in this set
     */
    public IntegerSet complement(IntegerSet intSetb) {
        return intSetb.diff(this);
    }

    /**
     * Checks if the set is empty.
     *
     * @return true if the set has no elements, false otherwise
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * Returns a string representation of the set in ascending order.
     * Format: [1, 2, 3]
     *
     * @return a formatted string representation of the set
     */
    @Override
    public String toString() {
        ArrayList<Integer> copy = new ArrayList<>(set);
        Collections.sort(copy);

        StringBuilder sb = new StringBuilder();
        sb.append("[");

        for (int i = 0; i < copy.size(); i++) {
            sb.append(copy.get(i));
            if (i < copy.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

}

package com.siwind.routingloop;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairInt implements WritableComparable<PairInt> {

    private int first = 0;
    private int second = 0;

    public PairInt() {

    }

    public PairInt(int first, int second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Set the left and right values.
     */
    public void set(int left, int right) {
        first = left;
        second = right;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    /**
     * Read the two integers. Encoded as: MIN_VALUE -> 0, 0 -> -MIN_VALUE,
     * MAX_VALUE-> -1
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt() + Integer.MIN_VALUE;
        second = in.readInt() + Integer.MIN_VALUE;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first - Integer.MIN_VALUE);
        out.writeInt(second - Integer.MIN_VALUE);
    }

    @Override
    public int hashCode() {
        return first * 157 + second;
    }

    @Override
    public boolean equals(Object right) {
        if (right instanceof PairInt) {
            PairInt r = (PairInt) right;
            return r.first == first && r.second == second;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(PairInt o) {
        if (first != o.first) {
            return first < o.first ? -1 : 1;
        } else if (second != o.second) {
            return second < o.second ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public PairInt clone() {
        // TODO Auto-generated method stub
        return new PairInt(first, second);
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return first + "-" + second;
    }

    /**
     * A Comparator that compares serialized IntPair.
     */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PairInt.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

}

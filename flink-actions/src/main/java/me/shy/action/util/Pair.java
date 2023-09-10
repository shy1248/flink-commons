package me.shy.action.util;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public class Pair<L, R> implements Map.Entry<L, R>, Serializable {

    private static final long serialVersionUID = 1L;

    /** Left object. */
    private L left;

    /** Right object. */
    private R right;

    public static <L, R> Pair<L, R> of(L left, R right) {
        return new Pair<>(left, right);
    }

    private Pair(final L left, final R right) {
        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    public void setLeft(L left) {
        this.left = left;
    }

    public R setRight(R right) {
        R previous = this.right;
        this.right = right;
        return previous;
    }

    @Override
    public L getKey() {
        return getLeft();
    }

    @Override
    public R getValue() {
        return getRight();
    }

    @Override
    public R setValue(R value) {
        return setRight(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return Objects.equals(left, pair.left) && Objects.equals(right, pair.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public String toString() {
        return "(" + getLeft() + ',' + getRight() + ')';
    }
}

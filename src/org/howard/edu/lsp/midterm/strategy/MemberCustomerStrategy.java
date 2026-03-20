package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for members (10% discount).
 */
public class MemberCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 10% discount for members.
     */
    @Override
    public double calculate(double price) {
        return price * 0.90;
    }
}

package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for VIP customers (20% discount).
 */
public class VIPCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 20% discount for VIP customers.
     */
    @Override
    public double calculate(double price) {
        return price * 0.80;
    }
}

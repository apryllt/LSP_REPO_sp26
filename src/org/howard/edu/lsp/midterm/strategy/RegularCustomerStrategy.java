package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for regular customers (no discount).
 */
public class RegularCustomerStrategy implements PriceStrategy {

    /**
     * Returns the price unchanged for regular customers.
     */
    @Override
    public double calculate(double price) {
        return price;
    }
}

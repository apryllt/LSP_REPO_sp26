package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for VIP customers (20% discount).
 * @author Aprille Thomas
 */
public class VIPCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 20% discount to the original price.
     *
     * @param price The original price.
     * @return The price after applying a 20% discount.
     */
    @Override
    public double calculate(double price) {
        return price * 0.80;
    }
}

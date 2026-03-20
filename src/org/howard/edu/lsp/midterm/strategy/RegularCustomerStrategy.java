package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for regular customers (no discount).
 * @author Aprille Thomas
 */
public class RegularCustomerStrategy implements PriceStrategy {

    /**
     * Returns the price unchanged for regular customers.
     *
     * @param price The original price.
     * @return The same price without any discount.
     */
    @Override
    public double calculate(double price) {
        return price;
    }
}

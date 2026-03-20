package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy interface for calculating discounts.
 */
public interface PriceStrategy {

    /**
     * Calculates the final price based on the strategy.
     *
     * @param price The original price.
     * @return The final price after applying the discount.
     */
    double calculate(double price);
}

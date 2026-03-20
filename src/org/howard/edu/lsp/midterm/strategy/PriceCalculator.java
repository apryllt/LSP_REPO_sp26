package org.howard.edu.lsp.midterm.strategy;

/**
 * Context class that uses a PriceStrategy to calculate final prices.
 *@author Aprille Thomas
 */
public class PriceCalculator {

    private PriceStrategy strategy;

    /**
     * Sets the pricing strategy dynamically.
     *
     * @param strategy The pricing strategy to use.
     */
    public void setStrategy(PriceStrategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Calculates the final price using the current strategy.
     *
     * @param price The original price.
     * @return The final price after applying the strategy.
     * @throws IllegalStateException if no PriceStrategy has been set.
     */
    public double calculatePrice(double price) {
        if (strategy == null) {
            throw new IllegalStateException("PriceStrategy is not set");
        }
        return strategy.calculate(price);
    }
}

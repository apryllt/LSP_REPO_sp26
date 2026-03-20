package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for holiday promotion (15% discount).
 */
public class HolidayCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 15% discount for holiday promotion.
     */
    @Override
    public double calculate(double price) {
        return price * 0.85;
    }
}

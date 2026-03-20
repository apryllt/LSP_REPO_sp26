package org.howard.edu.lsp.midterm.strategy;

/**
 * Strategy for members (10% discount).
 *@author Aprille Thomas
 */
public class MemberCustomerStrategy implements PriceStrategy {

    /**
     * Applies a 10% discount to the original price.
     *
     * @param price The original price.
     * @return The price after applying a 10% discount.
     */
    @Override
    public double calculate(double price) {
        return price * 0.90;
    }
}

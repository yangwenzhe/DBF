package entity;

public class flow_t {
    public int from;/* Feature number in signature 1 */
    public int to;/* Feature number in signature 2 */
    public double amount;/* amount from signature 1 to signature 2*/

    public flow_t(int from, int to, double amount) {
        this.from = from;
        this.to = to;
        this.amount = amount;
    }
    public flow_t() {
    }
}

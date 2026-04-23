Part 1:
Shared Resource #1: nextID
Shared Resource #2: requests
Concurrency Problem: because there are no checks ensuring singular access to the shared resource at a time, more then one thread may try to access a resource at the same time leading to race conditions and inconsistent data like duplicate ids.
Why addRequest() is unsafe: addRequest() is unsafe because it is using the getNextId() and requests.add() without proper syncronozation. Multiple thrreads calling this function may end up with duplicate ids ot the data being lost due to overwrites from the other threads.

Part 2:
Fix A: making this function synchronized means that when one thread gets hold of the shared resource nextID, it is locked and cannot be accessed by other threads. Thus eliminating the chance that 2 threads read the same vslue and assign it to two different students.
Fix B: making this function synchronized means that when one thread gets hold of the shared resource requests, it is locked and cannot be accessed by other threads. Thus eliminating the chance that 2 threads will try to write at the same time a result in a loss of data.
Fix C: Since getRequests only returns values, there is no shared resource being used and synchronizing this function will not fix the issue.

Part 3:
Based on Author Riel's heuristics getNextId() should be private because we want code to be encapsulated and only make known what is absolutely necessary. The logic for generating a new id should not be be globally available.

Part 4:
Description:
Instead of using the synchronized keyword, an AtomicInteger could have been used. Unlike synchronization which locks the whole function, the atomic variable internally manages individual operations where the specific variable is accessed making the code more efficient.

Code Snippet:
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

private AtomicInteger nextId = new AtomicInteger(1);
private List<String> requests = Collections.synchronizedList(new ArrayList<>());

public void addRequest(String studentName) {
    int id = nextId.getAndIncrement();
    requests.add("Request-" + id + " from " + studentName);
}

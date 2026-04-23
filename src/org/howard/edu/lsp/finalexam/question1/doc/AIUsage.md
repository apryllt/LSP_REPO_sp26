AI Tools Used:
ChatGPT

Prompts Used (2–5 max):
1. are nextID and requests shared resources here?:
import java.util.ArrayList;
import java.util.List;

public class RequestManager {
    private int nextId = 1;
    private List<String> requests = new ArrayList<>();

    public int getNextId() {
        int id = nextId;
        nextId++;
        return id;
    }

    public void addRequest(String studentName) {
        int id = getNextId();
        String request = "Request-" + id + " from " + studentName;
        requests.add(request);
    }

    public List<String> getRequests() {
        return requests;
    }
} 

2. is this a suitable answer for what concurrency problem may occur: because there are no checks ensuring singular access to the shared resource at a time, more then one function may try to access a resource at the same time leading to race conditions and incorrect data.

3. why is the addRequest func unsafe?

4. how would you implement sunchronization here?

5. if I only added synchronization to 1 of the functions would it work?

How AI Helped (2–3 sentences):
Using AI helped me clarify my answers by pointing out blurry logic or hazy terminology. It also helped me to understand the distinctions between similar concepts.

Reflection (1–2 sentences):
I was able to understand how atomic variables make code more efficient than using synchronization. 

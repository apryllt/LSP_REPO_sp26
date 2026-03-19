package org.howard.edu.lsp.midterm.crccards;

import java.util.*;

/**
 * Manages a collection of Task objects.
 * Provides functionality to add, find, and filter tasks.
 * 
 * @author Aprille Thomas
 */

public class TaskManager {
    private Map<String, Task> tasks;

    /**
     * Constructs a TaskManager.
     */
    public TaskManager() {
        tasks = new HashMap<>();
    }

    /**
     * Adds a new task.
     * Throws exception if duplicate ID exists.
     * 
     * @param task the task to add
     * @throws IllegalArgumentException if task ID already exists
     */
    public void addTask(Task task) {
        if (tasks.containsKey(task.getTaskId())) {
            throw new IllegalArgumentException("Duplicate task ID");
        }
        tasks.put(task.getTaskId(), task);
    }

    /**
     * Finds a task by its ID.
     * 
     * @param taskId the ID to search for
     * @return the Task if found, otherwise null
     */
    public Task findTask(String taskId) {
        return tasks.get(taskId);
    }

    /**
     * Returns all tasks matching a given status.
     * 
     * @param status the status to filter by
     * @return list of matching tasks
     */
    public List<Task> getTasksByStatus(String status) {
        List<Task> result = new ArrayList<>();

        for (Task task : tasks.values()) {
            if (task.getStatus().equals(status)) { // case-sensitive
                result.add(task);
            }
        }

        return result;
    }
}

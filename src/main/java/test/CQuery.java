package test;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

public class CQuery {

    Map<Integer, JSONObject> tree;
    Queue<JSONObject> queue;
    int next;
    int last;
    public CQuery(JSONArray jsonArray) {
        this.queue = new LinkedList<>();
        jsonArray.forEach(jsonObj -> {
            JSONObject jsonObject = (JSONObject) jsonObj;

            queue.add(jsonObject);
        });

//        this.tree = new HashMap<>();
//        jsonArray.forEach(jsonObj -> {
//            JSONObject jsonObject = (JSONObject) jsonObj;
//
//            int id = jsonObject.getIntValue("id");
//
//            if (id > next) {
//                next = id;
//            }
//            if (id < last) {
//                last = id;
//            }
//
//            tree.put(id, jsonObject);
//        });
    }

    public JSONObject getNext () {

        return queue.poll();

//        JSONObject jsonObj = tree.get(next);
//
//        next = jsonObj.getIntValue("id_a", -1);
//
//        if (next == -1) {
//            // another table scan
//
//        } else {
//            return jsonObj;
//        }

    }

    public boolean isNotDone () {
        return !queue.isEmpty();
    }
}

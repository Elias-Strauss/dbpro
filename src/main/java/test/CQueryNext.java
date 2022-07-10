package test;

import com.alibaba.fastjson2.JSONObject;

public class CQueryNext {
    private final int parameter;
    private final JSONObject jsonObj;

    public CQueryNext(int path, JSONObject jsonObj) {
        this.parameter = path;
        this.jsonObj = jsonObj;
    }

    public int getParameter() {
        return parameter;
    }

    public JSONObject getJsonObj() {
        return jsonObj;
    }
}

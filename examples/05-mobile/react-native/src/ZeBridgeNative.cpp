#include <jsi/jsi.h>
#include <ReactCommon/CallInvoker.h>
#include <string>

// Include the ZeBridge C ABI
extern "C" {
  uint64_t zb_client_open(const char* opts_json);
  int      zb_client_close(uint64_t handle);
  char*    zb_client_sync(uint64_t h);
  char*    zb_client_query(uint64_t h, const char* sql, const char* params_json);
  char*    zb_client_mutate(uint64_t h, const char* table, const char* op, const char* key_json, const char* values_json);
  char*    zb_client_poll(uint64_t h, uint64_t wait_ms);
  char*    zb_client_flush(uint64_t h, uint64_t wait_ms);
  void     zb_free(char* p);
}

using namespace facebook;

/**
 * Example JSI Binding for ZeBridge.
 * In a real TurboModule, this would be registered inside the React Native HostObject.
 */
class ZeBridgeHostObject : public jsi::HostObject {
public:
    ZeBridgeHostObject() {}

    jsi::Value get(jsi::Runtime &rt, const jsi::PropNameID &name) override {
        auto propName = name.utf8(rt);

        if (propName == "open") {
            return jsi::Function::createFromHostFunction(rt, name, 1,
                [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value {
                    std::string opts = args[0].asString(rt).utf8(rt);
                    uint64_t handle = zb_client_open(opts.c_str());
                    return jsi::Value((double)handle);
                });
        }
        
        if (propName == "sync") {
            return jsi::Function::createFromHostFunction(rt, name, 1,
                [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value {
                    uint64_t handle = (uint64_t)args[0].asNumber();
                    char* res = zb_client_sync(handle);
                    if (!res) return jsi::String::createFromUtf8(rt, "{\"error\":\"Unknown error\"}");
                    auto jsiStr = jsi::String::createFromUtf8(rt, res);
                    zb_free(res);
                    return jsiStr;
                });
        }
        
        if (propName == "query") {
            return jsi::Function::createFromHostFunction(rt, name, 3,
                [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value {
                    uint64_t handle = (uint64_t)args[0].asNumber();
                    std::string sql = args[1].asString(rt).utf8(rt);
                    std::string params = args[2].asString(rt).utf8(rt);
                    
                    char* res = zb_client_query(handle, sql.c_str(), params.c_str());
                    auto jsiStr = jsi::String::createFromUtf8(rt, res);
                    zb_free(res);
                    return jsiStr;
                });
        }

        if (propName == "mutate") {
            return jsi::Function::createFromHostFunction(rt, name, 5,
                [](jsi::Runtime &rt, const jsi::Value &thisVal, const jsi::Value *args, size_t count) -> jsi::Value {
                    uint64_t handle = (uint64_t)args[0].asNumber();
                    std::string table = args[1].asString(rt).utf8(rt);
                    std::string op = args[2].asString(rt).utf8(rt);
                    std::string key = args[3].asString(rt).utf8(rt);
                    std::string values = args[4].asString(rt).utf8(rt);
                    
                    char* res = zb_client_mutate(handle, table.c_str(), op.c_str(), key.c_str(), values.c_str());
                    auto jsiStr = jsi::String::createFromUtf8(rt, res);
                    zb_free(res);
                    return jsiStr;
                });
        }
        
        // For pollAsync and flushAsync, you would return a JSI Promise that
        // executes the blocking C call on a background std::thread and uses the
        // CallInvoker to resolve the Promise on the JS thread.

        return jsi::Value::undefined();
    }
};

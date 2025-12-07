# Build

```
git clone https://github.com/antsmallant/luaprofile.git
cd luaprofile && git submodule update --init && sh build.sh
```

---

# Example

## run script
```
cd example && sh run-example-profile.sh
```

## view result 

The result is a json file : "example_result.json". We can view it in a better way using https://jsongrid.com/ . Just open the file, and paste the content to jsongrid.    

![view-json-via-jsongrid](doc/view-json-via-jsongrid.png)

---

# 项目说明

参考自 https://github.com/lvzixun/luaprofile ，原始版本是 https://github.com/lsg2020/luaprofile 。  

主要修改点：   
1. fix 了 LUA_VLCF 类型函数没有正确获取 prototype 而导致统计出错的问题（具体见 `_get_prototype` 函数）。
2. 支持函数级别的内存 profile 。  
3. 增加必要的统计项。  

---

# Credits

* lvzixun https://github.com/lvzixun/luaprofile
* lsg2020 https://github.com/lsg2020/luaprofile





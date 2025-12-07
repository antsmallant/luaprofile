# Build

```
git clone https://github.com/antsmallant/luaprofile.git
cd luaprofile && sh build.sh
```

---

# Example

## run script
```
cd example && sh run-example-profile.sh
```

## view result 

The result is a json file : example_result.json .
We can view it in a better way using https://jsongrid.com/ .
Just open the file, and paste the content to jsongrid, then we can see the cost and the children. 

![view-json-via-jsongrid](doc/view-json-via-jsongrid.png)

---

# 项目说明

主要参考自 https://github.com/lvzixun/luaprofile ，原始版本是 https://github.com/lsg2020/luaprofile 。  

主要修改点：   
1. `_get_prototype` 精确的获取各种类型的函数原型，解决了 LUA_VLCF （比如 number/print 或自定义的c函数）这类函数没有正确获取函数 proto，而出现的统计错误问题。  

2. 支持函数级别的内存 profile 。  

---

# Credits

lvzixun https://github.com/lvzixun/luaprofile

lsg2020 https://github.com/lsg2020/luaprofile





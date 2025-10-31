对这个模块的类型标注做一下更新：
- 把Optional[x] 类型标注改为 x | None
- 把List[x, y] 类型标注改为 list[x, y]
- 把Dict[x, y] 类型标注改为 dict[x, y]
- 把Set[x] 类型标注改为 set[x]

---------

修改一下日志打印的风格，参考：
```python
logger.info(
    "用户登录成功",
    extra={
        "user_id": 12345,
        "username": "john_doe",
        "ip_address": "192.168.1.100",
        "login_time": time.time(),
    },
)
```
--------

为这个模块生成或更新CLAUDE.md文档

-----

为这个模块下的class成员和函数补全类型标注

对这个模块的类型标注做一下更新：
- 把Optional[x] 类型标注改为 x | None
- 把List[x, y] 类型标注改为 list[x, y]
- 把Dict[x, y] 类型标注改为 dict[x, y]
- 把Set[x] 类型标注改为 set[x]
- 把Tuple[x, y] 类型标注改为 tuple[x, y]
- 把Union[x, y] 类型标注改为 x | y
- 把any类型改为Any(from typing)

---------

修改这个模块的日志打印的风格，从：
```python
logger.info(
    f"this is a log: a={a}, "
    f"this is another log, b={b}, c={c}"
)
```
改为
```python
logger.info(
    "this is a log",
    extra={
        "a": a,
        "b": b,
        "c": c,
    },
)
```
--------

为这个模块生成或更新CLAUDE.md文档

-----

为这个模块下的class成员和函数补全类型标注，参考：
从
```python
class A:
    def __init__(self, a: int, b: str) -> None:
        self.a = a
        self.b = b

    def method(self, x, y) -> None:
        pass
```
改为
```python
class A:
    def __init__(self, a: int, b: str) -> None:
        self.a: str = a
        self.b: str = b

    def method(self, x: float, y: bool) -> None:
        pass
```

-----
重新组织一下这个模块下的依赖import顺序，让其有序易维护

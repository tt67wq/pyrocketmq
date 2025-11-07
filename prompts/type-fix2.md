为以下Python代码添加类型标注：

- 为类的构造函数`__init__`中的所有参数和类成员变量添加类型标注，要求每个参数和成员变量都明确指定其类型，例如：
  ```python
  class A:
      def __init__(self, a: str, b: int):
          self.a: str = a
          self.b: int = b
  ```
- 为模块中的所有函数添加完整的类型标注，包括：
  - 每个函数的参数需明确标注类型。
  - 函数的返回值需标注类型，使用`->`语法明确返回值类型。
    ```python
    def some_func(a: int, b: str | None) -> dict[str, str]:
        pass
    ```
- 为临时变量添加类型标注，例如：
  ```python
  x: int = 10
  y: str = "hello"
  z: list[int] = [1, 2, 3]
  ```
- 类型标注需符合PEP 484标准，并确保语法正确、无歧义。

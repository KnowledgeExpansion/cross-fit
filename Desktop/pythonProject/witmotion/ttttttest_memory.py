from memory_profiler import profile

@profile
def example_function():
    a = [1] * (10**6)  # 약 8MB 메모리 사용
    b = [2] * (2 * 10**7)  # 약 160MB 메모리 사용
    del b
    return a

if __name__ == "__main__":
    example_function()
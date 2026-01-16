#!/usr/bin/env python3
"""
EVE市场价格数据获取脚本
通过ESI API获取市场订单数据，过滤、分组并计算最高买价和最低卖价
"""

import aiohttp
import asyncio
import json
import os
from typing import List, Tuple, Dict, Any
from collections import defaultdict

# 请求头配置
HEADERS = {
    'Accept-Encoding': 'gzip,deflate'
}

# 超时配置
TIMEOUT = aiohttp.ClientTimeout(total=30)

# 目标系统ID
TARGET_SYSTEM_ID = 30000142


async def fetch_page(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, page: int) -> Tuple[int, List[Dict[str, Any]]]:
    """
    异步获取指定页面的订单数据
    
    Args:
        session: aiohttp会话对象
        semaphore: 信号量，用于控制并发数
        page: 页码
        
    Returns:
        (页码, 订单列表) 的元组
    """
    url = f'https://esi.evetech.net/markets/10000002/orders?order_type=all&page={page}'
    async with semaphore:
        try:
            async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()
                return (page, data)
        except Exception as e:
            print(f"获取第 {page} 页时出错: {e}")
            return (page, [])


async def get_total_pages(session: aiohttp.ClientSession) -> Tuple[int, List[Dict[str, Any]]]:
    """
    获取第一页数据并返回总页数和第一页数据
    
    Args:
        session: aiohttp会话对象
        
    Returns:
        (总页数, 第一页数据) 的元组
    """
    url = 'https://esi.evetech.net/markets/10000002/orders?order_type=all&page=1'
    async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:
        response.raise_for_status()
        
        # 从响应头获取总页数
        total_pages = int(response.headers.get('x-pages', 1))
        print(f"总页数: {total_pages}")
        
        # 获取第一页数据
        first_page_data = await response.json()
        return (total_pages, first_page_data)


async def fetch_all_pages(concurrent: int = 50) -> List[Dict[str, Any]]:
    """
    异步并发获取所有页面的订单数据
    
    Args:
        concurrent: 并发数量，默认50
        
    Returns:
        所有页面订单数据的合并列表
    """
    async with aiohttp.ClientSession() as session:
        # 获取总页数和第一页数据
        total_pages, first_page_data = await get_total_pages(session)
        all_data = first_page_data.copy()
        print(f"第 1 页: 获取到 {len(first_page_data)} 条订单")
        
        # 如果只有一页，直接返回
        if total_pages == 1:
            return all_data
        
        # 创建信号量控制并发数
        semaphore = asyncio.Semaphore(concurrent)
        
        # 创建所有页面的任务
        tasks = [
            fetch_page(session, semaphore, page)
            for page in range(2, total_pages + 1)
        ]
        
        # 并发执行所有任务
        results = await asyncio.gather(*tasks)
        
        # 按页码排序并合并数据
        results_dict = {page: data for page, data in results}
        for page in sorted(results_dict.keys()):
            data = results_dict[page]
            all_data.extend(data)
            print(f"第 {page} 页: 获取到 {len(data)} 条订单")
    
    return all_data


def process_orders(orders: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    """
    处理订单数据：过滤、分组并计算最高买价和最低卖价
    
    Args:
        orders: 订单列表
        
    Returns:
        处理后的数据列表，格式: [{"id": type_id, "b": buy_price, "s": sell_price}]
    """
    # 1. 过滤 system_id = 30000142 的订单
    filtered_orders = [order for order in orders if order.get('system_id') == TARGET_SYSTEM_ID]
    print(f"\n过滤后剩余 {len(filtered_orders)} 条订单（system_id={TARGET_SYSTEM_ID}）")
    
    # 2. 按 type_id 分组
    orders_by_type = defaultdict(list)
    for order in filtered_orders:
        type_id = order.get('type_id')
        if type_id:
            orders_by_type[type_id].append(order)
    
    print(f"共 {len(orders_by_type)} 种不同的 type_id")
    
    # 3. 对每个 type_id，按 is_buy_order 分组并计算价格
    result = []
    for type_id, type_orders in orders_by_type.items():
        buy_orders = [o for o in type_orders if o.get('is_buy_order') is True]
        sell_orders = [o for o in type_orders if o.get('is_buy_order') is False]
        
        # 计算最高买价（买单中价格最高的），如果没有买单则为0
        max_buy_price = max([o['price'] for o in buy_orders]) if buy_orders else 0
        
        # 计算最低卖价（卖单中价格最低的），如果没有卖单则为0
        min_sell_price = min([o['price'] for o in sell_orders]) if sell_orders else 0
        
        # 构建结果对象，始终包含 b 和 s 字段
        item = {
            "id": type_id,
            "b": max_buy_price,
            "s": min_sell_price
        }
        
        result.append(item)
    
    # 按 type_id 排序
    result.sort(key=lambda x: x['id'])
    
    return result


def save_data(data: List[Dict[str, int]], output_dir: str = "output"):
    """
    保存数据到文件
    
    Args:
        data: 要保存的数据列表
        output_dir: 输出目录
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存为JSON文件
    output_file = os.path.join(output_dir, "market_prices.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n数据已保存到: {output_file}")
    print(f"总共处理了 {len(data)} 种商品的价格数据")


async def main():
    """主函数"""
    print("开始获取EVE市场订单数据...")
    
    # 异步并发获取所有页面数据
    all_orders = await fetch_all_pages(concurrent=50)
    print(f"\n总共获取到 {len(all_orders)} 条订单")
    
    # 处理订单数据
    processed_data = process_orders(all_orders)
    
    # 保存数据
    save_data(processed_data)
    
    print("完成！")


if __name__ == "__main__":
    asyncio.run(main())


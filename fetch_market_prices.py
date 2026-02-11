#!/usr/bin/env python3
"""
EVE市场价格数据获取脚本
通过ESI API获取市场订单数据，过滤、分组并计算最高买价和最低卖价
"""

import aiohttp
import asyncio
import json
import os
import sys
from typing import List, Tuple, Dict, Any, Optional
from collections import defaultdict

# 请求头配置
HEADERS = {
    'Accept-Encoding': 'gzip,deflate'
}

# 超时配置
TIMEOUT = aiohttp.ClientTimeout(total=30)

# 目标系统ID
TARGET_SYSTEM_ID = 30000142

# 退出码：0=成功 1=真实故障(需报错) 2=维护中(不报错但不发布)
EXIT_SUCCESS = 0
EXIT_REAL_FAILURE = 1
EXIT_MAINTENANCE = 2


async def check_esi_status() -> Optional[int]:
    """
    请求 ESI 状态接口，解析 players。
    超时或无法解析时返回 None；成功时返回 players 数值（可能为 0）。
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://esi.evetech.net/status",
                headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as response:
                response.raise_for_status()
                data = await response.json()
                players = data.get("players")
                if players is not None and isinstance(players, (int, float)):
                    return int(players)
                return None
    except Exception:
        return None


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


def process_orders(orders: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    """
    处理订单数据：过滤、分组并计算最高买价和最低卖价
    
    Args:
        orders: 订单列表
        
    Returns:
        处理后的数据字典，格式: {type_id: {"b": buy_price, "s": sell_price}}
        其中buy_price或sell_price为0时不包含对应字段，两者都为0时不包含该条目
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
    result = {}
    for type_id, type_orders in orders_by_type.items():
        buy_orders = [o for o in type_orders if o.get('is_buy_order') is True]
        sell_orders = [o for o in type_orders if o.get('is_buy_order') is False]
        
        # 计算最高买价（买单中价格最高的），如果没有买单则为0
        max_buy_price = max([o['price'] for o in buy_orders]) if buy_orders else 0
        
        # 计算最低卖价（卖单中价格最低的），如果没有卖单则为0
        min_sell_price = min([o['price'] for o in sell_orders]) if sell_orders else 0
        
        # 如果两者都为0，则跳过该条目
        if max_buy_price == 0 and min_sell_price == 0:
            continue
        
        # 构建结果对象，只包含非0的字段
        item = {}
        if max_buy_price > 0:
            item["b"] = max_buy_price
        if min_sell_price > 0:
            item["s"] = min_sell_price
        
        # 将type_id转换为字符串作为key
        result[str(type_id)] = item
    
    return result


def save_data(data: Dict[str, Dict[str, int]], output_dir: str = "output"):
    """
    保存数据到文件
    
    Args:
        data: 要保存的数据字典，格式: {type_id: {"b": buy_price, "s": sell_price}}
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


async def main() -> int:
    """
    主函数。返回退出码：
    0=成功 1=真实故障(ESI 正常但数据接口失败) 2=维护中(不发布)
    """
    try:
        print("开始获取EVE市场订单数据...")

        # 异步并发获取所有页面数据
        all_orders = await fetch_all_pages(concurrent=50)
        print(f"\n总共获取到 {len(all_orders)} 条订单")

        # 处理订单数据
        processed_data = process_orders(all_orders)

        # 保存数据
        save_data(processed_data)

        print("完成！")
        return EXIT_SUCCESS
    except Exception as e:
        print(f"数据更新失败: {e}")
        players = await check_esi_status()
        if players is not None and players > 0:
            print("ESI 状态正常(players>0)，判定为真实故障，action 将报错。")
            return EXIT_REAL_FAILURE
        print("ESI 不可用或 players=0，判定为维护中，本次不发布 release。")
        return EXIT_MAINTENANCE


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)


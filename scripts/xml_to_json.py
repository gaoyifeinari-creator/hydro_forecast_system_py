#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import xml.etree.ElementTree as ET
import json
import os


# In[ ]:


def get_text(element, tag, default=None):
    """安全获取 XML 节点的文本值"""
    if element is None:
        return default
    node = element.find(tag)
    return node.text if node is not None else default

def get_bool(element, tag, default=None):
    """将 XML 中 true/false 文本转换为布尔值"""
    val = get_text(element, tag, default)
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    text = str(val).strip().lower()
    if text == 'true':
        return True
    if text == 'false':
        return False
    return default

def parse_section(section_node, down_sec_id=None):
    """
    解析 Section 节点 (包括 outSection 和 interSection)
    SecId 使用断面 ID（outSecId/interSecId），regionId 单独保留
    """
    region = section_node.find('region')
    if region is None:
        return None

    # 1. 基础信息：SecId 使用 section 级 ID，而不是 regionId
    sec_id_str = get_text(section_node, 'outSecId') or get_text(section_node, 'interSecId')
    sec_id = int(sec_id_str) if sec_id_str else None

    # 2. 提取数据源信息 (HFDataSource)
    ds_node = section_node.find('./HFDataSourceVec/HFDataSource')
    hf_data_source = {}
    if ds_node is not None:
        # 动态提取所有子节点
        for child in ds_node:
            hf_data_source[child.tag] = child.text

    # 3. 提取断面汇流参数（interSection 的 interSecCModel）
    sec_c_model_node = section_node.find('interSecCModel')
    sec_c_model = None
    if sec_c_model_node is not None:
        sec_c_model = {}
        for child in sec_c_model_node:
            val = child.text
            try:
                if '.' in val:
                    val = float(val)
                else:
                    val = int(val)
            except:
                pass
            sec_c_model[child.tag] = val

    sec_data = {
        "SecId": sec_id,
        "secType": get_text(section_node, 'secType'),
        "HFDispatchModelAlgType": get_text(section_node, 'HFDispatchModelAlgType'),
        "bHisCalcToPar": get_bool(section_node, 'bHisCalcToPar'),
        "bAllowRC": get_bool(section_node, 'bAllowRC'),
        "downSecId": down_sec_id,
        "secCModel": sec_c_model,
        "HFDataSource": hf_data_source,
        "units": []
    }

    # 4. 遍历单元 (unit)
    for unit_node in region.findall('unit'):
        # 提取单元模型参数 (unitGModel - 新安江模型等)
        g_model_node = unit_node.find('unitGModel')
        g_params = {}
        if g_model_node is not None:
            for child in g_model_node:
                # 尝试转换为数字，如果转换失败则保留字符串
                val = child.text
                try:
                    if '.' in val: val = float(val)
                    else: val = int(val)
                except: pass
                g_params[child.tag] = val

        # 提取汇流参数 (unitCModel - 马斯京根等)
        c_model_node = unit_node.find('unitCModel')
        c_params = {}
        if c_model_node is not None:
            for child in c_model_node:
                val = child.text
                try:
                    if '.' in val: val = float(val)
                    else: val = int(val)
                except: pass
                c_params[child.tag] = val

        # 提取蒸发站参数
        evap_node = unit_node.find('evapStation')
        evap_station = {
            "evapStaId": get_text(evap_node, 'evapStaId'),
            "evapStaName": get_text(evap_node, 'evapStaName'),
            "evapExtractType": get_text(evap_node, 'evapExtractType'),
            "evapStaSenid": get_text(evap_node, 'evapStaSenid'),
            "evapArr": get_text(evap_node, 'evapArr'),
            "evapStaWeight": float(get_text(evap_node, 'evapStaWeight', 0))
        }

        unit_data = {
            "unitId": int(get_text(unit_node, 'unitId')),
            "unitName": get_text(unit_node, 'unitName'),
            "preStations": [],
            "evapStation": evap_station,
            "unitGModel": g_params,
            "unitCModel": c_params
        }

        # 4. 提取降雨站 (preStation)
        for sta_node in unit_node.findall('preStation'):
            unit_data["preStations"].append({
                "preStaId": get_text(sta_node, 'preStaId'),
                "preStaName": get_text(sta_node, 'preStaName'),
                "preStaSenid": get_text(sta_node, 'preStaSenid'),
                "preStaWeight": float(get_text(sta_node, 'preStaWeight', 0))
            })

        sec_data["units"].append(unit_data)

    return sec_data

def xml_to_hierarchical_json_obj_by_timeType(xml_file_path):
    """按每个 scheme 的 timeType 分组输出所有断面（递归 interSection），返回 dict。"""
    if not os.path.exists(xml_file_path):
        raise FileNotFoundError(f"找不到文件: {xml_file_path}")

    tree = ET.parse(xml_file_path)
    root = tree.getroot()

    # foreSchemeSet 下的 scheme 节点（一般就是两个：RSDay/RSHour）
    scheme_nodes = root.findall('scheme')
    if not scheme_nodes:
        scheme_nodes = root.findall('.//scheme')

    result_by_time_type = {}

    def parse_recursive(section_node, down_sec_id):
        """解析一个 section 节点，并递归解析其下所有嵌套 interSection。"""
        sec_data = parse_section(section_node, down_sec_id=down_sec_id)
        if not sec_data:
            return []

        results = [sec_data]
        current_sec_id = sec_data.get("SecId")

        for inter_node in section_node.findall('interSection'):
            results.extend(parse_recursive(inter_node, down_sec_id=current_sec_id))

        return results

    for scheme_node in scheme_nodes:
        time_type = get_text(scheme_node, 'timeType')
        if time_type is None:
            time_type = 'unknown'

        out_section_nodes = scheme_node.findall('.//outSection')
        for out_section_node in out_section_nodes:
            sections = parse_recursive(out_section_node, down_sec_id=None)
            # 顶层使用 timeType 分组即可；不再把 timeType 写入每个断面对象
            result_by_time_type.setdefault(time_type, []).extend(sections)

    return result_by_time_type


def xml_to_hierarchical_json_by_timeType(xml_file_path, json_file_path):
    """按每个 scheme 的 timeType 分组输出所有断面（递归 interSection）。"""
    if not os.path.exists(xml_file_path):
        print(f"错误: 找不到文件 '{xml_file_path}'")
        return

    try:
        result_by_time_type = xml_to_hierarchical_json_obj_by_timeType(xml_file_path)

        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(result_by_time_type, f, ensure_ascii=False, indent=4)

        total = sum(len(v) for v in result_by_time_type.values())
        print(f"--- 转换成功 ---")
        print(f"输入文件: {xml_file_path}")
        print(f"输出文件: {json_file_path}")
        print(f"处理条目: {total} 个断面数据（按 timeType 分组）。")
        for k, v in result_by_time_type.items():
            print(f"  - {k}: {len(v)}")

    except Exception as e:
        import traceback
        print(f"--- 转换失败 ---")
        print(f"错误原因: {e}")
        traceback.print_exc()

# --- 执行 ---
if __name__ == "__main__":
    xml_to_hierarchical_json_by_timeType('HFSchemeConf.xml', 'result木里河.json')


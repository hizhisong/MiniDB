/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Longda on 2021/4/13.
//

#include <string>
#include <sstream>

#include "execute_stage.h"

#include "common/io/io.h"
#include "common/log/log.h"
#include "common/seda/timer_stage.h"
#include "common/lang/string.h"
#include "session/session.h"
#include "event/storage_event.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "event/execution_plan_event.h"
#include "sql/executor/execution_node.h"
#include "sql/executor/tuple.h"
#include "storage/common/table.h"
#include "storage/default/default_handler.h"
#include "storage/common/condition_filter.h"
#include "storage/trx/trx.h"

using namespace common;

RC create_selection_executor(Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node);

//! Constructor
ExecuteStage::ExecuteStage(const char *tag) : Stage(tag) {}

//! Destructor
ExecuteStage::~ExecuteStage() {}

//! Parse properties, instantiate a stage object
Stage *ExecuteStage::make_stage(const std::string &tag) {
  ExecuteStage *stage = new (std::nothrow) ExecuteStage(tag.c_str());
  if (stage == nullptr) {
    LOG_ERROR("new ExecuteStage failed");
    return nullptr;
  }
  stage->set_properties();
  return stage;
}

//! Set properties for this object set in stage specific properties
bool ExecuteStage::set_properties() {
  //  std::string stageNameStr(stageName);
  //  std::map<std::string, std::string> section = theGlobalProperties()->get(
  //    stageNameStr);
  //
  //  std::map<std::string, std::string>::iterator it;
  //
  //  std::string key;

  return true;
}

//! Initialize stage params and validate outputs
bool ExecuteStage::initialize() {
  LOG_TRACE("Enter");

  std::list<Stage *>::iterator stgp = next_stage_list_.begin();
  default_storage_stage_ = *(stgp++);
  mem_storage_stage_ = *(stgp++);

  LOG_TRACE("Exit");
  return true;
}

//! Cleanup after disconnection
void ExecuteStage::cleanup() {
  LOG_TRACE("Enter");

  LOG_TRACE("Exit");
}

void ExecuteStage::handle_event(StageEvent *event) {
  LOG_TRACE("Enter\n");

  handle_request(event);

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::callback_event(StageEvent *event, CallbackContext *context) {
  LOG_TRACE("Enter\n");

  // here finish read all data from disk or network, but do nothing here.
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SQLStageEvent *sql_event = exe_event->sql_event();
  sql_event->done_immediate();

  LOG_TRACE("Exit\n");
  return;
}

void ExecuteStage::handle_request(common::StageEvent *event) {
  ExecutionPlanEvent *exe_event = static_cast<ExecutionPlanEvent *>(event);
  SessionEvent *session_event = exe_event->sql_event()->session_event();
  Query *sql = exe_event->sqls();
  const char *current_db = session_event->get_client()->session->get_current_db().c_str();

  CompletionCallback *cb = new (std::nothrow) CompletionCallback(this, nullptr);
  if (cb == nullptr) {
    LOG_ERROR("Failed to new callback for ExecutionPlanEvent");
    exe_event->done_immediate();
    return;
  }
  exe_event->push_callback(cb);

  switch (sql->flag) {
    case SCF_SELECT: { // select
      RC rc = do_select(current_db, sql, exe_event->sql_event()->session_event());
      if (rc != RC::SUCCESS) {
          session_event->set_response("FAILURE\n");
      }
      exe_event->done_immediate();
    }
    break;

    case SCF_INSERT:
    case SCF_UPDATE:
    case SCF_DELETE:
    case SCF_CREATE_TABLE:
    case SCF_SHOW_TABLES:
    case SCF_DESC_TABLE:
    case SCF_DROP_TABLE:
    case SCF_CREATE_INDEX:
    case SCF_DROP_INDEX: 
    case SCF_LOAD_DATA: {
      StorageEvent *storage_event = new (std::nothrow) StorageEvent(exe_event);
      if (storage_event == nullptr) {
        LOG_ERROR("Failed to new StorageEvent");
        event->done_immediate();
        return;
      }

      default_storage_stage_->handle_event(storage_event);
    }
    break;
    case SCF_SYNC: {
      RC rc = DefaultHandler::get_default().sync();
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_BEGIN: {
      session_event->get_client()->session->set_trx_multi_operation_mode(true);
      session_event->set_response(strrc(RC::SUCCESS));
      exe_event->done_immediate();
    }
    break;
    case SCF_COMMIT: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->commit();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_ROLLBACK: {
      Trx *trx = session_event->get_client()->session->current_trx();
      RC rc = trx->rollback();
      session_event->get_client()->session->set_trx_multi_operation_mode(false);
      session_event->set_response(strrc(rc));
      exe_event->done_immediate();
    }
    break;
    case SCF_HELP: {
      const char *response = "show tables;\n"
          "desc `table name`;\n"
          "create table `table name` (`column name` `column type`, ...);\n"
          "create index `index name` on `table` (`column`);\n"
          "insert into `table` values(`value1`,`value2`);\n"
          "update `table` set column=value [where `column`=`value`];\n"
          "delete from `table` [where `column`=`value`];\n"
          "select [ * | `columns` ] from `table`;\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    case SCF_EXIT: {
      // do nothing
      const char *response = "Unsupported\n";
      session_event->set_response(response);
      exe_event->done_immediate();
    }
    break;
    default: {
      exe_event->done_immediate();
      LOG_ERROR("Unsupported command=%d\n", sql->flag);
    }
  }
}

void end_trx_if_need(Session *session, Trx *trx, bool all_right) {
  if (!session->is_trx_multi_operation_mode()) {
    if (all_right) {
      trx->commit();
    } else {
      trx->rollback();
    }
  }
}
void do_join_dfs(std::vector<TupleSet>& tuple_sets, int level, std::vector<const Tuple*> path_for_makeARecord, TupleSet& result) {
    // 递归终点
    if (path_for_makeARecord.size() >= tuple_sets.size()) {   // if 终点
        Tuple tuple;
        for (long unsigned int i = 0; i < path_for_makeARecord.size(); i++) {
            tuple.merge(*path_for_makeARecord[i]);
        }
        result.add(std::move(tuple));       // Add a tuple to final return
        return;
    }

    // 遍历一个Table的所有Tuple
    for (int i = 0; i < tuple_sets[level].size(); i++) {
        path_for_makeARecord.push_back(&tuple_sets[level].get(i)); // push an original tuple from tuple_sets
        do_join_dfs(tuple_sets, level-1, path_for_makeARecord, result);
        path_for_makeARecord.pop_back();
    }
}

bool match_table(const Selects &selects, const char *table_name_in_condition,  const char* const *table_name_to_match);

Record make_virtual_record(const char* data) {
    Record record;
    memset(&record, 0, sizeof(Record));
    record.data = (char*)(data);

    return record;
}

// 这里没有对输入的某些信息做合法性校验，比如查询的列名、where条件中的列名等，没有做必要的合法性校验
// 需要补充上这一部分. 校验部分也可以放在resolve，不过跟execution放一起也没有关系
// 单表多表查询逻辑合并
RC ExecuteStage::do_select(const char *db, Query *sql, SessionEvent *session_event) {

  RC rc = RC::SUCCESS;
  Session *session = session_event->get_client()->session;
  Trx *trx = session->current_trx();
  const Selects &selects = sql->sstr.selection;
  // 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
  std::vector<SelectExeNode *> select_nodes;
  for (size_t i = 0; i < selects.relation_num; i++) {
    const char *table_name = selects.relations[i];
    SelectExeNode *select_node = new SelectExeNode;
    rc = create_selection_executor(trx, selects, db, table_name, *select_node);
    if (rc != RC::SUCCESS) {
      delete select_node;
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    }
    select_nodes.push_back(select_node);
  }

  if (select_nodes.empty()) {
    LOG_ERROR("No table given");
    end_trx_if_need(session, trx, false);
    return RC::SQL_SYNTAX;
  }

  std::vector<TupleSet> tuple_sets;
  std::vector<std::vector<std::string>> tupleSets;
  for (SelectExeNode *&node: select_nodes) {
    TupleSet tuple_set;
    rc = node->execute(tuple_set);
    if (rc != RC::SUCCESS) {
      for (SelectExeNode *& tmp_node: select_nodes) {
        delete tmp_node;
      }
      end_trx_if_need(session, trx, false);
      return rc;
    } else {
      tuple_sets.push_back(std::move(tuple_set));
    }
  }

  TupleSet tuple_set;
  std::stringstream ss;
  if (tuple_sets.size() > 1) {
      // TODO 多张表合并后表如果都为空
      // TODO float过滤有问题

    // 本次查询了多张表，需要做join操作
    // 制作新属性表表头
    TupleSchema tuple_schema;
    for (int i = tuple_sets.size()-1; i >= 0; i--) {
        tuple_schema.merge(tuple_sets[i].get_schema());
    }
    tuple_set.set_schema(tuple_schema);

//    tuple_set.get_schema().print(ss);

    // 进行笛卡尔积，填充值
    std::vector<const Tuple*> path;
    do_join_dfs(tuple_sets, tuple_sets.size()-1, path, tuple_set);

    // 单表中两个属性间的比较 过滤-where
    // 找出仅与此表相关的过滤条件, 或者都是值的过滤条件
    DefaultConditionFilter* condition_filters[selects.condition_num];
    int condition_filters_count = 0, connection_table_size = 0;
    for (size_t i = 0; i < selects.condition_num; i++) {
        const Condition &condition = selects.conditions[i];
        if ((condition.left_is_attr == 1 && condition.right_is_attr == 1)) {
            if (::match_table(selects, condition.left_attr.relation_name, selects.relations) &&
                ::match_table(selects, condition.right_attr.relation_name, selects.relations)) {      // 左右都是属性名，并且表名都符合
                Table *table_left = DefaultHandler::get_default().find_table(db, condition.left_attr.relation_name);
                const FieldMeta *field_left = table_left->table_meta().field(condition.left_attr.attribute_name);
                Table *table_right = DefaultHandler::get_default().find_table(db, condition.right_attr.relation_name);
                const FieldMeta *field_right = table_right->table_meta().field(condition.right_attr.attribute_name);
                if (field_left->type() != field_right->type()) {
                    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
                }

                int left_offset = -1, right_offset = -1, offset = 0;
                for (int i = 0; i < tuple_set.schema().fields().size(); i++) {
                    TupleField tuple_field = tuple_set.schema().field(i);
                    if (::strcmp(tuple_field.table_name(), table_left->name()) == 0 &&
                        ::strcmp(tuple_field.field_name(), field_left->name()) == 0 &&
                        tuple_field.type() == field_left->type()) {
                        left_offset = offset;
                    } else if (::strcmp(tuple_field.table_name(), table_right->name()) == 0 &&
                               ::strcmp(tuple_field.field_name(), field_right->name()) == 0 &&
                               tuple_field.type() == field_right->type()) {
                        right_offset = offset;
                    }
                    offset += tuple_set.get(0).get(i).size();
                }

                connection_table_size = offset;

                if (left_offset == -1 || right_offset == -1) {
                    return RC::GENERIC_ERROR;
                }

                DefaultConditionFilter *condition_filter = new DefaultConditionFilter();        // TODO delete
                ConDesc left = {true, tuple_set.get(0).size(), left_offset, nullptr};
                ConDesc right = {true, tuple_set.get(0).size(), right_offset, nullptr};
                RC rc = condition_filter->init(left, right, field_left->type(), condition.comp);

                // 不存在的属性return

                if (rc != RC::SUCCESS) {
                    delete condition_filter;
                    for (DefaultConditionFilter *&filter: condition_filters) {
                        delete filter;
                    }
                    return rc;
                }
                condition_filters[condition_filters_count++] = condition_filter;
            } else {
                return RC::GENERIC_ERROR;
            }
        }
    }


    CompositeConditionFilter compositeConditionFilter;
    compositeConditionFilter.init((const ConditionFilter**)condition_filters, condition_filters_count);
    for(std::vector<Tuple>::const_iterator it = tuple_set.tuples().begin(); it != tuple_set.tuples().end(); ){
      char* data = new char[connection_table_size];
      auto& set = it->values();
      size_t offset = 0;
      for (int i = 0; i < set.size(); i++) {
          memcpy(data + offset, set[i]->to_string().c_str(), set[i]->size());
          offset += set[i]->size();
      }

      Record record = make_virtual_record(data);

      if (!compositeConditionFilter.filter(record)) {
          it = tuple_set.remove(it);
      } else {
          it++;
      }
    }

    // clear condition_filters

  } else {
    tuple_set = std::move(tuple_sets.front());
  }

  // group by

  // aggregation

  // order by

	// 去除表中多余属性
	for (int i = selects.attr_num - 1; i >= 0; i--) {
	    const RelAttr &attr = selects.attributes[i];
	    if (nullptr == attr.relation_name) {
	        if (0 == strcmp("*", attr.attribute_name)) {
	            // 这张表的所有属性
	            continue;
	        }
	    } else {

      }
	}

  // print/子查询
  if (tuple_sets.size() > 1) {
    tuple_set.print(ss, true);
  } else {
    tuple_set.print(ss, false);
  }

  for (SelectExeNode *& tmp_node: select_nodes) {
    delete tmp_node;
  }
  session_event->set_response(ss.str());
  end_trx_if_need(session, trx, true);
  return rc;
}

bool match_table(const Selects &selects, const char *table_name_in_condition,  const char* const *table_name_to_match){
    if (table_name_in_condition != nullptr) {
      while (*table_name_to_match != nullptr) {
          if (0 == strcmp(table_name_in_condition, *table_name_to_match)) {
                return true;
          }
          table_name_to_match++;
      }
  }

  return selects.relation_num == 1;
}

static RC schema_add_field(Table *table, const char *field_name, TupleSchema &schema) {
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
    LOG_WARN("No such field. %s.%s", table->name(), field_name);
    return RC::SCHEMA_FIELD_MISSING;
  }

  schema.add_if_not_exists(field_meta->type(), table->name(), field_meta->name());
  return RC::SUCCESS;
}

// 把所有的表和只跟这张表关联的condition都拿出来，生成最底层的select 执行节点
RC create_selection_executor(Trx *trx, const Selects &selects, const char *db, const char *table_name, SelectExeNode &select_node) {
  // 列出跟这张表关联的Attr
  TupleSchema schema;
  Table * table = DefaultHandler::get_default().find_table(db, table_name);
  if (nullptr == table) {
    LOG_WARN("No such table [%s] in db [%s]", table_name, db);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

//  for (int i = selects.attr_num - 1; i >= 0; i--) {
//    const RelAttr &attr = selects.attributes[];
//    if (nullptr == attr.relation_name || 0 == strcmp(table_name, attr.relation_name)) {
//      if (0 == strcmp("*", attr.attribute_name)) {
        // 列出这张表所有字段
        TupleSchema::from_table(table, schema);
//        break; // 没有校验，给出* 之后，再写字段的错误
//      } else {
//        // 列出这张表相关字段
//        RC rc = schema_add_field(table, attr.attribute_name, schema);
//        if (rc != RC::SUCCESS) {
//          return rc;
//        }
//      }
//    }
//  }

  // 找出仅与此表相关的过滤条件, 或者都是值的过滤条件
  std::vector<DefaultConditionFilter *> condition_filters;
  const char* table_names[] = {table_name};
  for (size_t i = 0; i < selects.condition_num; i++) {
    const Condition &condition = selects.conditions[i];
    if ((condition.left_is_attr == 0 && condition.right_is_attr == 0) || // 两边都是值
        (condition.left_is_attr == 1 && condition.right_is_attr == 0 && match_table(selects, condition.left_attr.relation_name, table_names)) ||  // 左边是属性右边是值
        (condition.left_is_attr == 0 && condition.right_is_attr == 1 && match_table(selects, condition.right_attr.relation_name, table_names)) ||  // 左边是值，右边是属性名
        (condition.left_is_attr == 1 && condition.right_is_attr == 1 &&
            match_table(selects, condition.left_attr.relation_name, table_names) && match_table(selects, condition.right_attr.relation_name, table_names)) // 左右都是属性名，并且表名都符合
        ) {
      DefaultConditionFilter *condition_filter = new DefaultConditionFilter();
      RC rc = condition_filter->init(*table, condition);
      if (rc != RC::SUCCESS) {
        delete condition_filter;
        for (DefaultConditionFilter * &filter : condition_filters) {
          delete filter;
        }
        return rc;
      }
      condition_filters.push_back(condition_filter);
    }
  }

  return select_node.init(trx, table, std::move(schema), std::move(condition_filters));
}

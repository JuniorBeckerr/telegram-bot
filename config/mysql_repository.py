import pytz
from datetime import datetime
import json
import logging
from typing import List, Dict, Any, Tuple, Optional, Union
from config.mysql import MySQL

logger = logging.getLogger(__name__)

utc_now = datetime.now(pytz.UTC)
BRASIL_TIMEZONE = pytz.timezone('America/Sao_Paulo')

brasil_now = utc_now.astimezone(BRASIL_TIMEZONE)


class QueryBuilder:
    def __init__(self, table_name, repository=None):
        self.table_name = table_name
        self.repository = repository
        self.conditions = []
        self.order_clauses = []
        self.group_clauses = []
        self.having_clauses = []
        self.skip_value = 0
        self.limit_value = 0
        self.select_columns = ['*']
        self.joins = []
        self.params = []

    def where(self, field, value=None, operator='='):
        """Adiciona uma condição de filtro"""
        # Se apenas dois argumentos foram fornecidos, ajusta os parâmetros (estilo Laravel)
        if value is None and operator == '=':
            # where('active') - Verifica se o campo é verdadeiro
            self.conditions.append(f"{field} = %s")
            self.params.append(True)
        elif value is None:
            # where('email', '=') - operador como segundo argumento
            value = operator
            operator = '='
            self.conditions.append(f"{field} {operator} %s")
            self.params.append(value)
        else:
            # where('age', '>', 18) - uso normal
            self.conditions.append(f"{field} {operator} %s")
            self.params.append(value)
        return self

    def where_in(self, field, values):
        """Filtro para valores em uma lista"""
        if not values:
            # Se a lista estiver vazia, adicionamos uma condição que nunca será verdadeira
            self.conditions.append("1=0")
            return self

        placeholders = ', '.join(['%s'] * len(values))
        self.conditions.append(f"{field} IN ({placeholders})")
        self.params.extend(values)
        return self

    def whereIn(self, field, values):
        """Alias para where_in no estilo camelCase do Laravel"""
        return self.where_in(field, values)

    def where_not_in(self, field, values):
        """Filtro para valores não em uma lista"""
        if not values:
            # Se a lista estiver vazia, retornamos todos os registros
            return self

        placeholders = ', '.join(['%s'] * len(values))
        self.conditions.append(f"{field} NOT IN ({placeholders})")
        self.params.extend(values)
        return self

    def whereNotIn(self, field, values):
        """Alias para where_not_in no estilo camelCase do Laravel"""
        return self.where_not_in(field, values)

    def where_between(self, field, start, end):
        """Filtro para valores entre dois valores"""
        self.conditions.append(f"{field} BETWEEN %s AND %s")
        self.params.extend([start, end])
        return self

    def whereBetween(self, field, values):
        """Alias para where_between no estilo camelCase do Laravel"""
        if isinstance(values, (list, tuple)) and len(values) == 2:
            return self.where_between(field, values[0], values[1])
        else:
            raise ValueError("O parâmetro 'values' para whereBetween deve ser uma lista ou tupla com 2 elementos")

    def where_not_between(self, field, start, end):
        """Filtro para valores que não estão entre dois valores"""
        self.conditions.append(f"{field} NOT BETWEEN %s AND %s")
        self.params.extend([start, end])
        return self

    def whereNotBetween(self, field, values):
        """Alias para where_not_between no estilo camelCase do Laravel"""
        if isinstance(values, (list, tuple)) and len(values) == 2:
            return self.where_not_between(field, values[0], values[1])
        else:
            raise ValueError("O parâmetro 'values' para whereNotBetween deve ser uma lista ou tupla com 2 elementos")

    def where_greater_than(self, field, value):
        """Filtro para valores maiores que"""
        return self.where(field, value, '>')

    def whereGt(self, field, value):
        """Alias para where_greater_than"""
        return self.where_greater_than(field, value)

    def where_greater_than_or_equal(self, field, value):
        """Filtro para valores maiores ou iguais a"""
        return self.where(field, value, '>=')

    def whereGte(self, field, value):
        """Alias para where_greater_than_or_equal"""
        return self.where_greater_than_or_equal(field, value)

    def where_less_than(self, field, value):
        """Filtro para valores menores que"""
        return self.where(field, value, '<')

    def whereLt(self, field, value):
        """Alias para where_less_than"""
        return self.where_less_than(field, value)

    def where_less_than_or_equal(self, field, value):
        """Filtro para valores menores ou iguais a"""
        return self.where(field, value, '<=')

    def whereLte(self, field, value):
        """Alias para where_less_than_or_equal"""
        return self.where_less_than_or_equal(field, value)

    def where_like(self, field, pattern):
        """Filtro para padrões de texto (LIKE do SQL)"""
        self.conditions.append(f"{field} LIKE %s")
        self.params.append(f"%{pattern}%")
        return self

    def whereLike(self, field, pattern):
        """Alias para where_like"""
        return self.where_like(field, pattern)

    def where_not_like(self, field, pattern):
        """Filtro para padrões de texto que não correspondem (NOT LIKE do SQL)"""
        self.conditions.append(f"{field} NOT LIKE %s")
        self.params.append(f"%{pattern}%")
        return self

    def whereNotLike(self, field, pattern):
        """Alias para where_not_like"""
        return self.where_not_like(field, pattern)

    def where_null(self, field):
        """Filtro para campos nulos"""
        self.conditions.append(f"{field} IS NULL")
        return self

    def whereNull(self, field):
        """Alias para where_null"""
        return self.where_null(field)

    def where_not_null(self, field):
        """Filtro para campos não nulos"""
        self.conditions.append(f"{field} IS NOT NULL")
        return self

    def whereNotNull(self, field):
        """Alias para where_not_null"""
        return self.where_not_null(field)

    def or_where(self, field, value=None, operator='='):
        """Adiciona uma condição OR"""
        # Verifica se temos condições existentes
        if not self.conditions:
            return self.where(field, value, operator)

        # Ajusta os parâmetros com base na quantidade de argumentos
        if value is None and operator == '=':
            condition = f"{field} = %s"
            self.params.append(True)
        elif value is None:
            value = operator
            condition = f"{field} = %s"
            self.params.append(value)
        else:
            condition = f"{field} {operator} %s"
            self.params.append(value)

        # Junta as condições existentes em um grupo
        existing_conditions = " AND ".join(f"({c})" for c in self.conditions)
        self.conditions = [f"({existing_conditions}) OR {condition}"]

        return self

    def orWhere(self, field, value=None, operator='='):
        """Alias para or_where"""
        return self.or_where(field, value, operator)

    def where_raw(self, raw_condition, *params):
        """Adiciona uma condição SQL bruta"""
        self.conditions.append(raw_condition)
        if params:
            self.params.extend(params)
        return self

    def whereRaw(self, raw_condition, *params):
        """Alias para where_raw"""
        return self.where_raw(raw_condition, *params)

    def order_by(self, field, direction='asc'):
        """Adiciona ordenação"""
        direction = direction.upper()
        if direction not in ['ASC', 'DESC']:
            direction = 'ASC'
        self.order_clauses.append(f"{field} {direction}")
        return self

    def orderBy(self, field, direction='asc'):
        """Alias para order_by"""
        return self.order_by(field, direction)

    def latest(self, field='created_at'):
        """Ordenar por um campo em ordem decrescente (mais recente primeiro)"""
        return self.order_by(field, 'desc')

    def oldest(self, field='created_at'):
        """Ordenar por um campo em ordem ascendente (mais antigo primeiro)"""
        return self.order_by(field, 'asc')

    def group_by(self, *fields):
        """Adiciona agrupamento"""
        self.group_clauses.extend(fields)
        return self

    def groupBy(self, *fields):
        """Alias para group_by"""
        return self.group_by(*fields)

    def having(self, condition, *params):
        """Adiciona condição HAVING para agrupamentos"""
        self.having_clauses.append(condition)
        if params:
            self.params.extend(params)
        return self

    def join(self, table, first, operator, second, join_type='INNER'):
        """Adiciona JOIN à query"""
        join_types = ['INNER', 'LEFT', 'RIGHT', 'FULL']
        if join_type.upper() not in join_types:
            join_type = 'INNER'

        self.joins.append(f"{join_type} JOIN {table} ON {first} {operator} {second}")
        return self

    def left_join(self, table, first, operator, second):
        """Adiciona LEFT JOIN à query"""
        return self.join(table, first, operator, second, 'LEFT')

    def leftJoin(self, table, first, operator, second):
        """Alias para left_join"""
        return self.left_join(table, first, operator, second)

    def right_join(self, table, first, operator, second):
        """Adiciona RIGHT JOIN à query"""
        return self.join(table, first, operator, second, 'RIGHT')

    def rightJoin(self, table, first, operator, second):
        """Alias para right_join"""
        return self.right_join(table, first, operator, second)

    def skip(self, value):
        """Define o OFFSET para paginação"""
        self.skip_value = int(value)
        return self

    def take(self, value):
        """Define o LIMIT de registros (alias para limit)"""
        return self.limit(value)

    def limit(self, value):
        """Define o LIMIT de registros"""
        self.limit_value = int(value)
        return self

    def select(self, *fields):
        """Define os campos a serem retornados"""
        if fields:
            self.select_columns = fields
        return self

    def _build_query(self, for_count=False):
        """Constrói a query SQL com base nos parâmetros"""
        if for_count:
            query = f"SELECT COUNT(*) as count FROM `{self.table_name}`"
        else:
            columns = ', '.join(self.select_columns)
            query = f"SELECT {columns} FROM `{self.table_name}`"

        # Adiciona JOINs
        if self.joins:
            query += " " + " ".join(self.joins)

        # Adiciona condições WHERE
        if self.conditions:
            query += " WHERE " + " AND ".join(self.conditions)

        # Adiciona GROUP BY
        if self.group_clauses and not for_count:
            query += " GROUP BY " + ", ".join(self.group_clauses)

        # Adiciona HAVING
        if self.having_clauses and not for_count:
            query += " HAVING " + " AND ".join(self.having_clauses)

        # Adiciona ORDER BY
        if self.order_clauses and not for_count:
            query += " ORDER BY " + ", ".join(self.order_clauses)

        # Adiciona LIMIT e OFFSET
        if self.limit_value > 0 and not for_count:
            query += f" LIMIT {self.limit_value}"

            if self.skip_value > 0:
                query += f" OFFSET {self.skip_value}"

        return query

    def get(self):
        """Executa a query e retorna múltiplos registros"""
        connection = None
        cursor = None
        try:
            query = self._build_query()
            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, self.params)
            results = cursor.fetchall()

            return results
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def all(self):
        """Alias para get() - Retorna todos os registros com os filtros aplicados"""
        return self.get()

    def first(self):
        """Retorna o primeiro registro"""
        original_limit = self.limit_value
        self.limit_value = 1
        results = self.get()
        self.limit_value = original_limit
        return results[0] if results else None

    def find(self, id):
        """Busca por ID"""
        original_conditions = self.conditions.copy()
        original_params = self.params.copy()

        self.conditions = []
        self.params = []

        if self.repository:
            primary_key = self.repository.primary_key
        else:
            primary_key = 'id'

        self.where(primary_key, id)
        result = self.first()

        # Restaura as condições originais
        self.conditions = original_conditions
        self.params = original_params

        return result

    def find_or_fail(self, id):
        """Busca por ID e gera erro se não encontrar"""
        result = self.find(id)
        if result is None:
            raise Exception(f"Registro com ID {id} não encontrado")
        return result

    def findOrFail(self, id):
        """Alias para find_or_fail"""
        return self.find_or_fail(id)

    def count(self):
        """Conta os registros"""
        connection = None
        cursor = None
        try:
            query = self._build_query(for_count=True)
            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, self.params)
            result = cursor.fetchone()

            return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Erro ao contar registros: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def exists(self):
        """Verifica se existe algum registro"""
        return self.count() > 0

    def doesnt_exist(self):
        """Verifica se não existe nenhum registro"""
        return not self.exists()

    def doesntExist(self):
        """Alias para doesnt_exist"""
        return self.doesnt_exist()

    def _execute_aggregate(self, aggregate_function, field):
        """Executa função de agregação"""
        connection = None
        cursor = None
        try:
            query = f"SELECT {aggregate_function}({field}) as aggregate FROM `{self.table_name}`"

            # Adiciona JOINs
            if self.joins:
                query += " " + " ".join(self.joins)

            # Adiciona condições WHERE
            if self.conditions:
                query += " WHERE " + " AND ".join(self.conditions)

            # Adiciona GROUP BY
            if self.group_clauses:
                query += " GROUP BY " + ", ".join(self.group_clauses)

            # Adiciona HAVING
            if self.having_clauses:
                query += " HAVING " + " AND ".join(self.having_clauses)

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, self.params)
            result = cursor.fetchone()

            return result['aggregate'] if result and 'aggregate' in result else None
        except Exception as e:
            logger.error(f"Erro ao executar agregação: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def sum(self, field):
        """Calcula a soma de um campo"""
        return self._execute_aggregate("SUM", field) or 0

    def avg(self, field):
        """Calcula a média de um campo"""
        return self._execute_aggregate("AVG", field) or 0

    def min(self, field):
        """Encontra o valor mínimo de um campo"""
        return self._execute_aggregate("MIN", field)

    def max(self, field):
        """Encontra o valor máximo de um campo"""
        return self._execute_aggregate("MAX", field)

    def pluck(self, value_column, key_column=None):
        """
        Retorna uma lista de valores de uma coluna ou um dicionário
        com chave e valor de duas colunas
        """
        if key_column:
            self.select_columns = [key_column, value_column]
            results = self.get()
            return {row[key_column]: row[value_column] for row in results}
        else:
            self.select_columns = [value_column]
            results = self.get()
            return [row[value_column] for row in results]

    def chunk(self, count, callback):
        """
        Processa os resultados em chunks para reduzir o uso de memória

        Args:
            count: Tamanho do chunk
            callback: Função que recebe cada chunk de resultados
        """
        offset = 0
        while True:
            query_clone = QueryBuilder(self.table_name)
            query_clone.conditions = self.conditions.copy()
            query_clone.params = self.params.copy()
            query_clone.select_columns = self.select_columns.copy()
            query_clone.joins = self.joins.copy()
            query_clone.order_clauses = self.order_clauses.copy()

            query_clone.skip(offset).limit(count)
            results = query_clone.get()

            if not results:
                break

            callback(results)
            offset += count

            if len(results) < count:
                break

    def paginate(self, page=1, per_page=15):
        """Retorna resultados paginados"""
        skip_value = (page - 1) * per_page
        items = self.skip(skip_value).limit(per_page).get()
        total = self.count()

        return {
            'items': items,
            'total': total,
            'page': page,
            'per_page': per_page,
            'pages': (total + per_page - 1) // per_page
        }

    def insert(self, data):
        """
        Insere um novo registro

        Args:
            data: Dicionário com os dados a serem inseridos

        Returns:
            ID do registro inserido
        """
        if self.repository:
            return self.repository.create(data)
        else:
            try:
                # Prepara os dados para inserção
                now = brasil_now.strftime('%Y-%m-%d %H:%M:%S')
                if 'created_at' not in data:
                    data['created_at'] = now
                if 'updated_at' not in data:
                    data['updated_at'] = now

                # Constrói a query INSERT
                fields = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))

                query = f"INSERT INTO `{self.table_name}` ({fields}) VALUES ({placeholders})"
                values = list(data.values())

                connection = MySQL.get_connection()
                cursor = connection.cursor()

                cursor.execute(query, values)
                connection.commit()

                # Obtém o ID inserido
                inserted_id = cursor.lastrowid

                return inserted_id
            except Exception as e:
                logger.error(f"Erro ao inserir registro: {e}")
                raise
            finally:
                try:
                    if cursor:
                        cursor.close()
                finally:
                    if connection:
                        connection.close()

    def update(self, data):
        """
        Atualiza registros com base nas condições definidas

        Args:
            data: Dicionário com os dados a serem atualizados

        Returns:
            Número de registros afetados
        """
        connection = None
        cursor = None
        try:
            # Adiciona timestamp de atualização
            update_data = data.copy()
            if 'updated_at' not in update_data:
                update_data['updated_at'] = brasil_now.strftime('%Y-%m-%d %H:%M:%S')

            # Constrói a query UPDATE
            set_clause = ', '.join([f"{field} = %s" for field in update_data.keys()])
            update_values = list(update_data.values())

            query = f"UPDATE `{self.table_name}` SET {set_clause}"
            params = update_values

            # Adiciona condições WHERE
            if self.conditions:
                query += " WHERE " + " AND ".join(self.conditions)
                params.extend(self.params)

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, params)
            connection.commit()

            affected_rows = cursor.rowcount
            return affected_rows
        except Exception as e:
            logger.error(f"Erro ao atualizar registros: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def delete(self):
        """
        Deleta registros com base nas condições definidas

        Returns:
            Número de registros afetados
        """
        connection = None
        cursor = None
        try:
            query = f"DELETE FROM `{self.table_name}`"

            # Adiciona condições WHERE
            if self.conditions:
                query += " WHERE " + " AND ".join(self.conditions)

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, self.params)
            connection.commit()

            affected_rows = cursor.rowcount
            return affected_rows
        except Exception as e:
            logger.error(f"Erro ao deletar registros: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def insert_get_id(self, data):
        """Insere um registro e retorna o ID inserido"""
        return self.insert(data)

    def insertGetId(self, data):
        """Alias para insert_get_id"""
        return self.insert_get_id(data)

    def value(self, column):
        """Retorna um único valor de uma coluna"""
        original_select = self.select_columns
        original_limit = self.limit_value

        self.select_columns = [column]
        self.limit_value = 1

        result = self.get()

        self.select_columns = original_select
        self.limit_value = original_limit

        return result[0][column] if result else None

    def when(self, condition, true_callback, false_callback=None):
        """
        Condicional para aplicar diferentes callbacks baseado em uma condição

        Args:
            condition: Condição booleana
            true_callback: Função a ser chamada se condition for True
            false_callback: Função a ser chamada se condition for False

        Returns:
            self
        """
        if condition:
            if callable(true_callback):
                true_callback(self)
        elif false_callback and callable(false_callback):
            false_callback(self)

        return self

    def tap(self, callback):
        """
        Executa uma função com a instância atual e retorna a instância

        Args:
            callback: Função que recebe a instância atual

        Returns:
            self
        """
        callback(self)
        return self

    def dump(self):
        """Imprime a query SQL e seus parâmetros para debug"""
        query = self._build_query()
        logger.debug(f"Query: {query}")
        logger.debug(f"Params: {self.params}")
        return self

    def where_column(self, field1, field2, operator='='):
        """Compara coluna com coluna (ex: attempts < max_attempts)"""
        self.conditions.append(f"{field1} {operator} {field2}")
        return self

    def whereColumn(self, field1, field2, operator='='):
        return self.where_column(field1, field2, operator)

    def or_where_column(self, field1, field2, operator='='):
        """OR comparando coluna com coluna"""
        if not self.conditions:
            return self.where_column(field1, field2, operator)

        existing = " AND ".join(f"({c})" for c in self.conditions)
        self.conditions = [f"({existing}) OR {field1} {operator} {field2}"]
        return self

    def orWhereColumn(self, field1, field2, operator='='):
        return self.or_where_column(field1, field2, operator)


class BaseRepository:
    def __init__(self, table_name, primary_key='id'):
        self.table_name = table_name
        self.primary_key = primary_key

    def query(self):
        """Inicia um query builder"""
        return QueryBuilder(self.table_name, self)

    def where(self, field, value=None, operator='='):
        """Atalho para iniciar uma query com where"""
        return self.query().where(field, value, operator)

    def select(self, *fields):
        """Atalho para iniciar uma query com select"""
        return self.query().select(*fields)

    def table(self):
        """Retorna uma instância do QueryBuilder"""
        return self.query()

    def all(self):
        """Retorna todos os registros"""
        return self.query().get()

    def find(self, id):
        """Busca por ID"""
        return self.query().find(id)

    def find_or_fail(self, id):
        """Busca por ID e gera erro se não encontrar"""
        return self.query().find_or_fail(id)

    def findOrFail(self, id):
        """Alias para find_or_fail"""
        return self.find_or_fail(id)

    def first(self):
        """Retorna o primeiro registro"""
        return self.query().first()

    def order_by(self, field, direction='asc'):
        """Atalho para iniciar uma query com order_by"""
        return self.query().order_by(field, direction)

    def orderBy(self, field, direction='asc'):
        """Alias para order_by"""
        return self.order_by(field, direction)

    def latest(self, field='created_at'):
        """Ordenar por um campo em ordem decrescente (mais recente primeiro)"""
        return self.query().latest(field)

    def oldest(self, field='created_at'):
        """Ordenar por um campo em ordem ascendente (mais antigo primeiro)"""
        return self.query().oldest(field)

    def _prepare_for_insert(self, data):
        """Prepara dados para inserção adicionando timestamps"""
        if 'created_at' not in data:
            data['created_at'] = brasil_now.strftime('%Y-%m-%d %H:%M:%S')
        if 'updated_at' not in data:
            data['updated_at'] = brasil_now.strftime('%Y-%m-%d %H:%M:%S')
        return data

    def _prepare_for_update(self, data):
        """Prepara dados para atualização"""
        data['updated_at'] = brasil_now.strftime('%Y-%m-%d %H:%M:%S')
        return data

    def create(self, data):
        """Cria um novo registro"""
        connection = None
        cursor = None
        try:
            data = self._prepare_for_insert(data)

            # Constrói a query INSERT
            fields = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))

            query = f"INSERT INTO `{self.table_name}` ({fields}) VALUES ({placeholders})"
            values = list(data.values())

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, values)
            connection.commit()

            # Obtém o ID inserido
            inserted_id = cursor.lastrowid

            # Retorna os dados com o ID
            data[self.primary_key] = inserted_id
            return data
        except Exception as e:
            logger.error(f"Erro ao criar registro: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def insert(self, data):
        """Alias para create, retorna apenas o ID"""
        result = self.create(data)
        return result[self.primary_key]

    def insert_get_id(self, data):
        """Alias para insert"""
        return self.insert(data)

    def insertGetId(self, data):
        """Alias para insert"""
        return self.insert(data)

    def update(self, id, data):
        """Atualiza um registro"""
        connection = None
        cursor = None
        try:
            data = self._prepare_for_update(data)

            # Constrói a query UPDATE
            set_clause = ', '.join([f"{field} = %s" for field in data.keys()])
            query = f"UPDATE `{self.table_name}` SET {set_clause} WHERE {self.primary_key} = %s"

            values = list(data.values())
            values.append(id)

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, values)
            connection.commit()

            affected_rows = cursor.rowcount
            return affected_rows > 0
        except Exception as e:
            logger.error(f"Erro ao atualizar registro: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def delete(self, id=None):
        """Deleta um registro ou registros baseados em query"""
        connection = None
        cursor = None
        try:
            if id is not None:
                # Deleta por ID
                query = f"DELETE FROM `{self.table_name}` WHERE {self.primary_key} = %s"
                values = [id]

                connection = MySQL.get_connection()
                cursor = connection.cursor()

                cursor.execute(query, values)
                connection.commit()

                affected_rows = cursor.rowcount

                return affected_rows > 0
            else:
                # Delega para QueryBuilder
                return self.query().delete()
        except Exception as e:
            logger.error(f"Erro ao deletar registro: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def paginate(self, page=1, per_page=15):
        """Retorna resultados paginados"""
        return self.query().paginate(page, per_page)

    def first_or_create(self, search_dict, defaults=None):
        """
        Busca o primeiro registro ou cria um novo se não existir

        Args:
            search_dict: Dicionário com critérios de busca
            defaults: Valores padrão para criar se não existir

        Returns:
            tuple: (registro, criado)
        """
        try:
            # Cria uma query com as condições de busca
            query_builder = self.query()
            for field, value in search_dict.items():
                query_builder.where(field, value)

            # Tenta encontrar o registro
            result = query_builder.first()

            if result:
                return result, False
            else:
                # Não encontrou, criar novo
                new_data = search_dict.copy()
                if defaults:
                    new_data.update(defaults)

                created = self.create(new_data)
                return created, True

        except Exception as e:
            logger.error(f"Erro em firstOrCreate: {e}")
            raise

    def firstOrCreate(self, search_dict, defaults=None):
        """Alias para first_or_create"""
        return self.first_or_create(search_dict, defaults)

    def first_or_new(self, search_dict, defaults=None):
        """
        Busca o primeiro registro ou retorna uma nova instância se não existir
        (sem salvar no banco)

        Args:
            search_dict: Dicionário com critérios de busca
            defaults: Valores padrão para o novo objeto

        Returns:
            dict: registro encontrado ou novo objeto
        """
        # Cria uma query com as condições de busca
        query_builder = self.query()
        for field, value in search_dict.items():
            query_builder.where(field, value)

        # Tenta encontrar o registro
        result = query_builder.first()

        if result:
            return result
        else:
            # Não encontrou, retorna novo objeto sem salvar
            new_data = search_dict.copy()
            if defaults:
                new_data.update(defaults)
            return new_data

    def firstOrNew(self, search_dict, defaults=None):
        """Alias para first_or_new"""
        return self.first_or_new(search_dict, defaults)

    def update_or_create(self, search_dict, update_dict):
        """
        Atualiza um registro existente ou cria um novo

        Args:
            search_dict: Critérios de busca
            update_dict: Dados para atualizar ou criar

        Returns:
            tuple: (registro, criado)
        """
        try:
            # Primeiro, busca o registro
            query_builder = self.query()
            for field, value in search_dict.items():
                query_builder.where(field, value)

            existing = query_builder.first()

            if existing:
                # Atualiza o registro existente
                id_value = existing[self.primary_key]
                self.update(id_value, update_dict)

                # Retorna o registro atualizado
                updated = self.find(id_value)
                return updated, False
            else:
                # Cria um novo registro
                new_data = {**search_dict, **update_dict}
                created = self.create(new_data)
                return created, True

        except Exception as e:
            logger.error(f"Erro em updateOrCreate: {e}")
            raise

    def updateOrCreate(self, search_dict, update_dict):
        """Alias para update_or_create"""
        return self.update_or_create(search_dict, update_dict)

    def update_or_insert(self, filter_dict, update_dict):
        """
        Atualiza ou insere (upsert)

        Args:
            filter_dict: Critérios de busca
            update_dict: Dados para atualizar

        Returns:
            dict: Informações sobre a operação
        """
        try:
            # Primeiro tenta buscar o registro
            query_builder = self.query()
            for field, value in filter_dict.items():
                query_builder.where(field, value)

            existing = query_builder.first()

            if existing:
                # Atualiza o registro existente
                id_value = existing[self.primary_key]
                self.update(id_value, update_dict)
                return {
                    'matched': 1,
                    'modified': 1,
                    'inserted_id': None
                }
            else:
                # Cria um novo registro
                insert_data = {**filter_dict, **update_dict}
                inserted_id = self.insert(insert_data)
                return {
                    'matched': 0,
                    'modified': 0,
                    'inserted_id': inserted_id
                }

        except Exception as e:
            logger.error(f"Erro em updateOrInsert: {e}")
            raise

    def updateOrInsert(self, filter_dict, update_dict):
        """Alias para update_or_insert"""
        return self.update_or_insert(filter_dict, update_dict)

    def find_one_and_update(self, filter_dict, update_dict, return_new=True):
        """
        Encontra e atualiza um registro, retornando o registro

        Args:
            filter_dict: Critérios de busca
            update_dict: Dados para atualizar
            return_new: Se True, retorna registro após atualização

        Returns:
            dict: Registro antes ou após atualização (baseado em return_new)
        """
        try:
            # Primeiro busca o registro
            query_builder = self.query()
            for field, value in filter_dict.items():
                query_builder.where(field, value)

            existing = query_builder.first()

            if not existing:
                return None

            # Guarda a versão original se necessário
            original = existing.copy() if not return_new else None

            # Atualiza o registro
            id_value = existing[self.primary_key]
            update_data = self._prepare_for_update(update_dict.copy())
            self.update(id_value, update_data)

            # Retorna o registro atualizado se necessário
            if return_new:
                return self.find(id_value)
            else:
                return original

        except Exception as e:
            logger.error(f"Erro em findOneAndUpdate: {e}")
            raise

    def findOneAndUpdate(self, filter_dict, update_dict, return_new=True):
        """Alias para find_one_and_update"""
        return self.find_one_and_update(filter_dict, update_dict, return_new)

    def increment(self, filter_dict, field, value=1):
        """
        Incrementa um campo numérico

        Args:
            filter_dict: Critérios de busca
            field: Campo para incrementar
            value: Valor para incrementar (pode ser negativo para decrementar)

        Returns:
            bool: True se sucesso
        """
        connection = None
        cursor = None
        try:
            # Constrói as condições WHERE
            conditions = []
            params = []
            for f, v in filter_dict.items():
                conditions.append(f"{f} = %s")
                params.append(v)

            where_clause = " AND ".join(conditions)

            # Constrói a query UPDATE
            query = f"""
                UPDATE `{self.table_name}`
                SET {field} = {field} + %s, updated_at = %s
                WHERE {where_clause}
            """

            # Adiciona os parâmetros
            params.insert(0, value)
            params.insert(1, brasil_now.strftime('%Y-%m-%d %H:%M:%S'))

            connection = MySQL.get_connection()
            cursor = connection.cursor()

            cursor.execute(query, params)
            connection.commit()

            affected_rows = cursor.rowcount

            return affected_rows > 0

        except Exception as e:
            logger.error(f"Erro em increment: {e}")
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def decrement(self, filter_dict, field, value=1):
        """Decrementa um campo numérico"""
        return self.increment(filter_dict, field, -value)

    def _get_json_field(self, field):
        """Retorna expressão SQL para acessar um campo JSON"""
        # MySQL usa -> ou ->> para acessar campos JSON
        return field

    def push(self, filter_dict, field, value):
        """
        Adiciona um valor a um campo JSON array

        Args:
            filter_dict: Critérios de busca
            field: Campo JSON array
            value: Valor para adicionar

        Returns:
            bool: True se sucesso
        """
        try:
            # Primeiro, busca o registro para obter o array atual
            query_builder = self.query()
            for f, v in filter_dict.items():
                query_builder.where(f, v)

            record = query_builder.first()
            if not record:
                return False

            # Obtém o array atual ou inicializa um novo
            current_array = record.get(field, [])
            if current_array is None:
                current_array = []
            elif isinstance(current_array, str):
                # Se estiver em formato string JSON, converte
                try:
                    current_array = json.loads(current_array)
                except:
                    current_array = []

            # Adiciona o novo valor e atualiza
            current_array.append(value)

            # Atualiza o registro
            update_data = {
                field: json.dumps(current_array) if isinstance(current_array, list) else current_array,
                'updated_at': brasil_now.strftime('%Y-%m-%d %H:%M:%S')
            }

            return self.update(record[self.primary_key], update_data)

        except Exception as e:
            logger.error(f"Erro em push: {e}")
            raise

    def pull(self, filter_dict, field, value):
        """
        Remove um valor de um campo JSON array

        Args:
            filter_dict: Critérios de busca
            field: Campo JSON array
            value: Valor para remover

        Returns:
            bool: True se sucesso
        """
        try:
            # Primeiro, busca o registro para obter o array atual
            query_builder = self.query()
            for f, v in filter_dict.items():
                query_builder.where(f, v)

            record = query_builder.first()
            if not record:
                return False

            # Obtém o array atual
            current_array = record.get(field, [])
            if current_array is None:
                return False
            elif isinstance(current_array, str):
                # Se estiver em formato string JSON, converte
                try:
                    current_array = json.loads(current_array)
                except:
                    return False

            # Remove o valor, se existir
            if value in current_array:
                current_array.remove(value)

            # Atualiza o registro
            update_data = {
                field: json.dumps(current_array) if isinstance(current_array, list) else current_array,
                'updated_at': brasil_now.strftime('%Y-%m-%d %H:%M:%S')
            }

            return self.update(record[self.primary_key], update_data)

        except Exception as e:
            logger.error(f"Erro em pull: {e}")
            raise

    def chunk(self, count, callback):
        """
        Processa os registros em chunks para reduzir o uso de memória

        Args:
            count: Tamanho do chunk
            callback: Função que recebe cada chunk de resultados
        """
        return self.query().chunk(count, callback)

    def bulk_update_or_create(self, data_list, unique_fields):
        """
        Atualiza ou cria múltiplos registros em bulk

        Args:
            data_list: Lista de registros
            unique_fields: Lista de campos que identificam unicidade

        Returns:
            dict: Estatísticas da operação
        """
        connection = None
        try:
            stats = {
                'matched': 0,
                'modified': 0,
                'upserted': 0
            }

            connection = MySQL.get_connection()

            # Processa cada registro individualmente (MySQL não tem bulk upsert como MongoDB)
            for data in data_list:
                # Cria filtro baseado nos campos únicos
                filter_dict = {field: data[field] for field in unique_fields if field in data}

                # Busca registro existente
                query_builder = self.query()
                for field, value in filter_dict.items():
                    query_builder.where(field, value)

                existing = query_builder.first()

                # Prepara dados para atualização/inserção
                update_data = data.copy()

                if existing:
                    # Atualiza registro existente
                    id_value = existing[self.primary_key]
                    update_data = self._prepare_for_update(update_data)

                    # Constrói a query UPDATE
                    set_clause = ', '.join([f"{field} = %s" for field in update_data.keys()])
                    query = f"UPDATE `{self.table_name}` SET {set_clause} WHERE {self.primary_key} = %s"

                    values = list(update_data.values())
                    values.append(id_value)

                    cursor = connection.cursor()
                    cursor.execute(query, values)

                    stats['matched'] += 1
                    if cursor.rowcount > 0:
                        stats['modified'] += 1

                    cursor.close()
                else:
                    # Insere novo registro
                    update_data = self._prepare_for_insert(update_data)

                    # Constrói a query INSERT
                    fields = ', '.join(update_data.keys())
                    placeholders = ', '.join(['%s'] * len(update_data))
                    query = f"INSERT INTO `{self.table_name}` ({fields}) VALUES ({placeholders})"

                    values = list(update_data.values())

                    cursor = connection.cursor()
                    cursor.execute(query, values)
                    stats['upserted'] += 1
                    cursor.close()

            # Commit as transações
            connection.commit()

            return stats

        except Exception as e:
            logger.error(f"Erro em bulkUpdateOrCreate: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def bulkUpdateOrCreate(self, data_list, unique_fields):
        """Alias para bulk_update_or_create"""
        return self.bulk_update_or_create(data_list, unique_fields)

    def count(self):
        """Conta todos os registros da tabela"""
        return self.query().count()
    def execute_raw(self, query, params=None):
        """
        Executa uma query SQL bruta

        Args:
            query: String SQL
            params: Parâmetros da query

        Returns:
            Resultado da execução
        """
        connection = None
        cursor = None
        try:
            connection = MySQL.get_connection()
            cursor = connection.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Verifica se é uma query SELECT
            if query.strip().upper().startswith('SELECT'):
                results = cursor.fetchall()
                return results
            else:
                connection.commit()
                affected_rows = cursor.rowcount
                return affected_rows

        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Erro ao executar query: {e}")
            # Se for erro de sequência de pacote, tenta resetar o pool
            if "Packet sequence number wrong" in str(e):
                logger.warning("Erro de sequência de pacote detectado - pode necessitar reset do pool")
                MySQL.reset_pool()
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
            finally:
                if connection:
                    connection.close()

    def executeRaw(self, query, params=None):
        """Alias para execute_raw"""
        return self.execute_raw(query, params)

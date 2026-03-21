import os
from datetime import datetime

import psycopg
from psycopg import errors
from flask import Flask, request, redirect, url_for, flash, render_template_string

app = Flask(__name__)
app.secret_key = "forrajeria_secret_key"

DATABASE_URL = os.environ.get("DATABASE_URL")
MARGEN_BOLSA = 0.30
MARGEN_KG = 0.40


class SQLiteLikeRow(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


def sqlite_like_row_factory(cursor):
    if cursor.description is None:
        return lambda values: values

    cols = [desc.name if hasattr(desc, "name") else desc[0] for desc in cursor.description]

    def make_row(values):
        return SQLiteLikeRow(zip(cols, values))

    return make_row

# =========================
# BASE DE DATOS
# =========================
class Database:
    def __init__(self, database_url=DATABASE_URL):
        self.database_url = database_url
        self.crear_tablas()
        self.migrar_tablas()

    def conectar(self):
        conn = psycopg.connect(self.database_url, row_factory=sqlite_like_row_factory)
        conn.autocommit = False
        return conn

    def crear_tablas(self):
        conn = self.conectar()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id BIGSERIAL PRIMARY KEY,
                codigo TEXT UNIQUE NOT NULL,
                nombre TEXT NOT NULL,
                categoria TEXT,
                unidad_base TEXT NOT NULL,
                es_fraccionado INTEGER NOT NULL DEFAULT 0,
                peso_bolsa REAL DEFAULT 0,
                precio_compra REAL NOT NULL DEFAULT 0,
                precio_venta REAL NOT NULL DEFAULT 0,
                precio_venta_bolsa REAL NOT NULL DEFAULT 0,
                precio_venta_kg REAL NOT NULL DEFAULT 0,
                stock REAL NOT NULL DEFAULT 0,
                stock_minimo REAL NOT NULL DEFAULT 0,
                activo INTEGER NOT NULL DEFAULT 1
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS proveedores (
                id BIGSERIAL PRIMARY KEY,
                nombre TEXT UNIQUE NOT NULL,
                telefono TEXT,
                direccion TEXT,
                observaciones TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS compras (
                id BIGSERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                proveedor_id INTEGER NOT NULL,
                total REAL NOT NULL,
                observaciones TEXT,
                estado TEXT NOT NULL DEFAULT 'ACTIVA',
                FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS detalle_compra (
                id BIGSERIAL PRIMARY KEY,
                compra_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                tipo_compra TEXT NOT NULL,
                cantidad REAL NOT NULL,
                costo_unitario REAL NOT NULL,
                subtotal REAL NOT NULL,
                FOREIGN KEY (compra_id) REFERENCES compras(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS ventas (
                id BIGSERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                total REAL NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS detalle_venta (
                id BIGSERIAL PRIMARY KEY,
                venta_id INTEGER NOT NULL,
                producto_id INTEGER NOT NULL,
                tipo_venta TEXT NOT NULL,
                cantidad REAL NOT NULL,
                precio_unitario REAL NOT NULL,
                subtotal REAL NOT NULL,
                FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS movimientos_stock (
                id BIGSERIAL PRIMARY KEY,
                fecha TEXT NOT NULL,
                producto_id INTEGER NOT NULL,
                tipo_movimiento TEXT NOT NULL,
                referencia TEXT,
                cantidad REAL NOT NULL,
                stock_anterior REAL NOT NULL,
                stock_resultante REAL NOT NULL,
                observaciones TEXT,
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        """)

        conn.commit()
        conn.close()

    def migrar_tablas(self):
        conn = self.conectar()
        cur = conn.cursor()

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'compras'
        """)
        cols = [r[0] for r in cur.fetchall()]
        if "estado" not in cols:
            cur.execute("ALTER TABLE compras ADD COLUMN estado TEXT NOT NULL DEFAULT 'ACTIVA'")

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos'
        """)
        cols = [r[0] for r in cur.fetchall()]
        if "activo" not in cols:
            cur.execute("ALTER TABLE productos ADD COLUMN activo INTEGER NOT NULL DEFAULT 1")

        conn.commit()
        conn.close()

    # ------------------------
    # Auxiliares
    # ------------------------
    def registrar_movimiento_stock(
        self, cur, producto_id, tipo_movimiento, referencia,
        cantidad, stock_anterior, stock_resultante, observaciones=""
    ):
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            INSERT INTO movimientos_stock (
                fecha, producto_id, tipo_movimiento, referencia, cantidad,
                stock_anterior, stock_resultante, observaciones
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            fecha, producto_id, tipo_movimiento, referencia, cantidad,
            stock_anterior, stock_resultante, observaciones
        ))

    def producto_tiene_operaciones(self, producto_id):
        conn = self.conectar()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM detalle_compra WHERE producto_id = %s", (producto_id,))
        compras = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM detalle_venta WHERE producto_id = %s", (producto_id,))
        ventas = cur.fetchone()[0]

        conn.close()
        return (compras + ventas) > 0

    # ------------------------
    # Productos
    # ------------------------
    def agregar_producto(self, datos):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO productos (
                    codigo, nombre, categoria, unidad_base, es_fraccionado, peso_bolsa,
                    precio_compra, precio_venta, precio_venta_bolsa, precio_venta_kg,
                    stock, stock_minimo, activo
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            """, datos)
            conn.commit()
            return True, "Producto agregado correctamente."
        except errors.UniqueViolation as e:
            return False, f"No se pudo guardar. {e}"
        except Exception as e:
            return False, f"Error al guardar producto: {e}"
        finally:
            conn.close()

    def actualizar_producto(self, producto_id, datos):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE productos
                SET codigo=%s, nombre=%s, categoria=%s, unidad_base=%s, es_fraccionado=%s, peso_bolsa=%s,
                    precio_compra=%s, precio_venta=%s, precio_venta_bolsa=%s, precio_venta_kg=%s,
                    stock=%s, stock_minimo=%s
                WHERE id=%s
            """, (*datos, producto_id))
            conn.commit()
            return True, "Producto actualizado correctamente."
        except errors.UniqueViolation as e:
            return False, f"No se pudo actualizar. {e}"
        except Exception as e:
            return False, f"Error al actualizar producto: {e}"
        finally:
            conn.close()

    def eliminar_producto(self, producto_id):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            if self.producto_tiene_operaciones(producto_id):
                return False, "El producto ya tiene compras o ventas. Conviene desactivarlo."

            cur.execute("DELETE FROM productos WHERE id = %s", (producto_id,))
            conn.commit()
            return True, "Producto eliminado correctamente."
        except Exception as e:
            return False, f"Error al eliminar producto: {e}"
        finally:
            conn.close()

    def cambiar_estado_producto(self, producto_id, activo):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("UPDATE productos SET activo = %s WHERE id = %s", (1 if activo else 0, producto_id))
            conn.commit()
            return True, "Estado actualizado."
        except Exception as e:
            return False, f"Error al cambiar estado: {e}"
        finally:
            conn.close()

    def obtener_productos(self, filtro="", solo_activos=False):
        conn = self.conectar()
        cur = conn.cursor()

        where = []
        params = []

        if filtro:
            like = f"%{filtro}%"
            where.append("(codigo LIKE %s OR nombre LIKE %s OR categoria LIKE %s)")
            params.extend([like, like, like])

        if solo_activos:
            where.append("activo = 1")

        where_sql = ""
        if where:
            where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT id, codigo, nombre, categoria, unidad_base, es_fraccionado, peso_bolsa,
                   precio_compra, precio_venta, precio_venta_bolsa, precio_venta_kg,
                   stock, stock_minimo, activo
            FROM productos
            {where_sql}
            ORDER BY nombre
        """, params)

        rows = cur.fetchall()
        conn.close()
        return rows

    def obtener_producto(self, producto_id):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, codigo, nombre, categoria, unidad_base, es_fraccionado, peso_bolsa,
                   precio_compra, precio_venta, precio_venta_bolsa, precio_venta_kg,
                   stock, stock_minimo, activo
            FROM productos
            WHERE id = %s
        """, (producto_id,))
        row = cur.fetchone()
        conn.close()
        return row

    def obtener_stock_bajo(self):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, codigo, nombre, unidad_base, es_fraccionado, stock, stock_minimo
            FROM productos
            WHERE stock <= stock_minimo AND activo = 1
            ORDER BY stock ASC, nombre ASC
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    # ------------------------
    # Proveedores
    # ------------------------
    def agregar_proveedor(self, datos):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO proveedores (nombre, telefono, direccion, observaciones)
                VALUES (%s, %s, %s, %s)
            """, datos)
            conn.commit()
            return True, "Proveedor agregado correctamente."
        except errors.UniqueViolation:
            return False, "Ya existe un proveedor con ese nombre."
        finally:
            conn.close()

    def obtener_proveedores(self):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, telefono, direccion, observaciones
            FROM proveedores
            ORDER BY nombre
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    def obtener_proveedor(self, proveedor_id):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, nombre, telefono, direccion, observaciones
            FROM proveedores
            WHERE id = %s
        """, (proveedor_id,))
        row = cur.fetchone()
        conn.close()
        return row

    def actualizar_proveedor(self, proveedor_id, datos):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("""
                UPDATE proveedores
                SET nombre=%s, telefono=%s, direccion=%s, observaciones=%s
                WHERE id=%s
            """, (*datos, proveedor_id))
            conn.commit()
            return True, "Proveedor actualizado correctamente."
        except errors.UniqueViolation:
            return False, "No se pudo actualizar."
        finally:
            conn.close()

    def eliminar_proveedor(self, proveedor_id):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM proveedores WHERE id = %s", (proveedor_id,))
            conn.commit()
            return True, "Proveedor eliminado."
        except Exception as e:
            return False, f"No se pudo eliminar: {e}"
        finally:
            conn.close()

    # ------------------------
    # Compras
    # ------------------------
    def registrar_compra(self, proveedor_id, items, observaciones):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            total = sum(item["subtotal"] for item in items)
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cur.execute("""
                INSERT INTO compras (fecha, proveedor_id, total, observaciones, estado)
                VALUES (%s, %s, %s, %s, 'ACTIVA')
                RETURNING id
            """, (fecha, proveedor_id, total, observaciones))
            compra_id = cur.fetchone()[0]

            for item in items:
                cur.execute("""
                    INSERT INTO detalle_compra (compra_id, producto_id, tipo_compra, cantidad, costo_unitario, subtotal)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    compra_id,
                    item["producto_id"],
                    item["tipo_compra"],
                    item["cantidad"],
                    item["costo_unitario"],
                    item["subtotal"]
                ))

                cur.execute("""
                    SELECT es_fraccionado, peso_bolsa, stock, precio_compra
                    FROM productos
                    WHERE id = %s
                """, (item["producto_id"],))
                prod_db = cur.fetchone()
                if not prod_db:
                    raise ValueError("Producto inexistente en compra.")

                es_fraccionado, peso_bolsa, stock_actual, precio_compra_actual = prod_db

                if es_fraccionado:
                    cantidad_base = item["cantidad"] * peso_bolsa
                    nuevo_stock = stock_actual + cantidad_base

                    costo_actual_por_kg = (precio_compra_actual / peso_bolsa) if peso_bolsa > 0 else 0
                    costo_nuevo_por_kg = (item["costo_unitario"] / peso_bolsa) if peso_bolsa > 0 else 0

                    if stock_actual <= 0:
                        costo_promedio_por_kg = costo_nuevo_por_kg
                    else:
                        costo_promedio_por_kg = (
                            (stock_actual * costo_actual_por_kg) +
                            (cantidad_base * costo_nuevo_por_kg)
                        ) / nuevo_stock

                    costo_bolsa = round(costo_promedio_por_kg * peso_bolsa, 2)
                    precio_bolsa = round(costo_bolsa * (1 + MARGEN_BOLSA), 2)
                    precio_kg = round(costo_promedio_por_kg * (1 + MARGEN_KG), 2)

                    cur.execute("""
                        UPDATE productos
                        SET stock = %s, precio_compra = %s, precio_venta_bolsa = %s, precio_venta_kg = %s
                        WHERE id = %s
                    """, (nuevo_stock, costo_bolsa, precio_bolsa, precio_kg, item["producto_id"]))

                    self.registrar_movimiento_stock(
                        cur,
                        item["producto_id"],
                        "COMPRA",
                        f"COMPRA #{compra_id}",
                        cantidad_base,
                        stock_actual,
                        nuevo_stock,
                        observaciones or "Compra registrada"
                    )
                else:
                    cantidad_base = item["cantidad"]
                    nuevo_stock = stock_actual + cantidad_base

                    if stock_actual <= 0:
                        costo_promedio = item["costo_unitario"]
                    else:
                        costo_promedio = (
                            (stock_actual * precio_compra_actual) +
                            (cantidad_base * item["costo_unitario"])
                        ) / nuevo_stock

                    costo_promedio = round(costo_promedio, 2)
                    precio_venta = round(costo_promedio * (1 + MARGEN_BOLSA), 2)

                    cur.execute("""
                        UPDATE productos
                        SET stock = %s, precio_compra = %s, precio_venta = %s
                        WHERE id = %s
                    """, (nuevo_stock, costo_promedio, precio_venta, item["producto_id"]))

                    self.registrar_movimiento_stock(
                        cur,
                        item["producto_id"],
                        "COMPRA",
                        f"COMPRA #{compra_id}",
                        cantidad_base,
                        stock_actual,
                        nuevo_stock,
                        observaciones or "Compra registrada"
                    )

            conn.commit()
            return True, f"Compra registrada. Total: ${total:,.2f}"
        except Exception as e:
            conn.rollback()
            return False, f"Error al registrar compra: {e}"
        finally:
            conn.close()

    def obtener_compras(self):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.id, c.fecha, p.nombre AS proveedor, c.total, COALESCE(c.observaciones, '') AS observaciones, c.estado
            FROM compras c
            INNER JOIN proveedores p ON p.id = c.proveedor_id
            ORDER BY c.id DESC
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    def obtener_detalle_compra(self, compra_id):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT pr.codigo, pr.nombre, dc.tipo_compra, dc.cantidad, dc.costo_unitario, dc.subtotal
            FROM detalle_compra dc
            INNER JOIN productos pr ON pr.id = dc.producto_id
            WHERE dc.compra_id = %s
            ORDER BY pr.nombre
        """, (compra_id,))
        rows = cur.fetchall()
        conn.close()
        return rows

    def anular_compra(self, compra_id):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("SELECT estado FROM compras WHERE id = %s", (compra_id,))
            fila = cur.fetchone()
            if not fila:
                return False, "La compra no existe."
            if fila[0] == "ANULADA":
                return False, "La compra ya está anulada."

            cur.execute("""
                SELECT dc.producto_id, dc.cantidad, p.es_fraccionado, p.peso_bolsa, p.stock, p.nombre
                FROM detalle_compra dc
                INNER JOIN productos p ON p.id = dc.producto_id
                WHERE dc.compra_id = %s
            """, (compra_id,))
            detalles = cur.fetchall()

            for producto_id, cantidad, es_fraccionado, peso_bolsa, stock_actual, nombre in detalles:
                descontar = cantidad * peso_bolsa if es_fraccionado else cantidad
                nuevo_stock = stock_actual - descontar

                if nuevo_stock < 0:
                    conn.rollback()
                    return False, f"No se puede anular porque '{nombre}' quedaría con stock negativo."

                cur.execute("UPDATE productos SET stock = %s WHERE id = %s", (nuevo_stock, producto_id))

                self.registrar_movimiento_stock(
                    cur,
                    producto_id,
                    "ANULACION_COMPRA",
                    f"COMPRA #{compra_id}",
                    -descontar,
                    stock_actual,
                    nuevo_stock,
                    "Compra anulada"
                )

            cur.execute("UPDATE compras SET estado = 'ANULADA' WHERE id = %s", (compra_id,))
            conn.commit()
            return True, "Compra anulada correctamente y stock revertido."
        except Exception as e:
            conn.rollback()
            return False, f"Error al anular compra: {e}"
        finally:
            conn.close()

    def eliminar_compra(self, compra_id):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("SELECT estado FROM compras WHERE id = %s", (compra_id,))
            fila = cur.fetchone()
            if not fila:
                return False, "La compra no existe."
            if fila[0] != "ANULADA":
                return False, "Solo se puede eliminar una compra anulada."

            cur.execute("DELETE FROM compras WHERE id = %s", (compra_id,))
            conn.commit()
            return True, "Compra eliminada."
        except Exception as e:
            conn.rollback()
            return False, f"Error al eliminar compra: {e}"
        finally:
            conn.close()

    # ------------------------
    # Ventas
    # ------------------------
    def registrar_venta(self, items):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            total = sum(item["subtotal"] for item in items)
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            cur.execute("INSERT INTO ventas (fecha, total) VALUES (%s, %s) RETURNING id", (fecha, total))
            venta_id = cur.fetchone()[0]

            for item in items:
                cur.execute("""
                    INSERT INTO detalle_venta (venta_id, producto_id, tipo_venta, cantidad, precio_unitario, subtotal)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    venta_id,
                    item["producto_id"],
                    item["tipo_venta"],
                    item["cantidad"],
                    item["precio_unitario"],
                    item["subtotal"]
                ))

                cur.execute("""
                    SELECT stock, es_fraccionado, peso_bolsa, nombre
                    FROM productos
                    WHERE id = %s
                """, (item["producto_id"],))
                prod_db = cur.fetchone()
                if not prod_db:
                    raise ValueError("Producto inexistente en venta.")

                stock_actual, es_fraccionado, peso_bolsa, nombre = prod_db

                if es_fraccionado:
                    descontar = item["cantidad"] * peso_bolsa if item["tipo_venta"] == "Bolsa" else item["cantidad"]
                else:
                    descontar = item["cantidad"]

                nuevo_stock = stock_actual - descontar
                if nuevo_stock < 0:
                    raise ValueError(f"Stock insuficiente para '{nombre}'.")

                cur.execute("UPDATE productos SET stock = %s WHERE id = %s", (nuevo_stock, item["producto_id"]))

                self.registrar_movimiento_stock(
                    cur,
                    item["producto_id"],
                    "VENTA",
                    f"VENTA #{venta_id}",
                    -descontar,
                    stock_actual,
                    nuevo_stock,
                    f"Venta en {item['tipo_venta']}"
                )

            conn.commit()
            return True, f"Venta registrada. Total: ${total:,.2f}"
        except Exception as e:
            conn.rollback()
            return False, f"Error al registrar venta: {e}"
        finally:
            conn.close()

    def obtener_ventas(self):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, fecha, total
            FROM ventas
            ORDER BY id DESC
        """)
        rows = cur.fetchall()
        conn.close()
        return rows

    def obtener_detalle_venta(self, venta_id):
        conn = self.conectar()
        cur = conn.cursor()
        cur.execute("""
            SELECT pr.codigo, pr.nombre, dv.tipo_venta, dv.cantidad, dv.precio_unitario, dv.subtotal
            FROM detalle_venta dv
            INNER JOIN productos pr ON pr.id = dv.producto_id
            WHERE dv.venta_id = %s
            ORDER BY pr.nombre
        """, (venta_id,))
        rows = cur.fetchall()
        conn.close()
        return rows

    def eliminar_venta(self, venta_id):
        conn = self.conectar()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT dv.producto_id, dv.tipo_venta, dv.cantidad, p.es_fraccionado, p.peso_bolsa, p.stock
                FROM detalle_venta dv
                INNER JOIN productos p ON p.id = dv.producto_id
                WHERE dv.venta_id = %s
            """, (venta_id,))
            detalles = cur.fetchall()

            if not detalles:
                return False, "La venta no existe o ya fue eliminada."

            for producto_id, tipo_venta, cantidad, es_fraccionado, peso_bolsa, stock_actual in detalles:
                if es_fraccionado:
                    devolver = cantidad * peso_bolsa if tipo_venta == "Bolsa" else cantidad
                else:
                    devolver = cantidad

                nuevo_stock = stock_actual + devolver

                cur.execute("UPDATE productos SET stock = %s WHERE id = %s", (nuevo_stock, producto_id))

                self.registrar_movimiento_stock(
                    cur,
                    producto_id,
                    "ELIMINACION_VENTA",
                    f"VENTA #{venta_id}",
                    devolver,
                    stock_actual,
                    nuevo_stock,
                    "Venta eliminada del registro"
                )

            cur.execute("DELETE FROM ventas WHERE id = %s", (venta_id,))
            conn.commit()
            return True, "Venta eliminada y stock devuelto."
        except Exception as e:
            conn.rollback()
            return False, f"Error al eliminar venta: {e}"
        finally:
            conn.close()

    # ------------------------
    # Movimientos
    # ------------------------
    def obtener_movimientos_stock(self, filtro=""):
        conn = self.conectar()
        cur = conn.cursor()

        if filtro:
            like = f"%{filtro}%"
            cur.execute("""
                SELECT ms.id, ms.fecha, p.codigo, p.nombre, ms.tipo_movimiento,
                       COALESCE(ms.referencia, '') AS referencia, ms.cantidad,
                       ms.stock_anterior, ms.stock_resultante,
                       COALESCE(ms.observaciones, '') AS observaciones
                FROM movimientos_stock ms
                INNER JOIN productos p ON p.id = ms.producto_id
                WHERE p.codigo LIKE %s OR p.nombre LIKE %s OR ms.tipo_movimiento LIKE %s OR ms.referencia LIKE %s
                ORDER BY ms.id DESC
            """, (like, like, like, like))
        else:
            cur.execute("""
                SELECT ms.id, ms.fecha, p.codigo, p.nombre, ms.tipo_movimiento,
                       COALESCE(ms.referencia, '') AS referencia, ms.cantidad,
                       ms.stock_anterior, ms.stock_resultante,
                       COALESCE(ms.observaciones, '') AS observaciones
                FROM movimientos_stock ms
                INNER JOIN productos p ON p.id = ms.producto_id
                ORDER BY ms.id DESC
            """)

        rows = cur.fetchall()
        conn.close()
        return rows

    # ------------------------
    # Dashboard
    # ------------------------
    def resumen_dashboard(self):
        conn = self.conectar()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM productos")
        productos = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM proveedores")
        proveedores = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM productos WHERE stock <= stock_minimo AND activo = 1")
        stock_bajo = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(total), 0) FROM ventas")
        total_ventas = cur.fetchone()[0]

        cur.execute("SELECT COALESCE(SUM(total), 0) FROM compras WHERE estado = 'ACTIVA'")
        total_compras = cur.fetchone()[0]

        conn.close()

        return {
            "productos": productos,
            "proveedores": proveedores,
            "stock_bajo": stock_bajo,
            "total_ventas": total_ventas,
            "total_compras": total_compras,
        }


db = Database()


# =========================
# TEMPLATE BASE
# =========================
BASE_HTML = """
<!doctype html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ titulo }}</title>
    <style>
        :root{
            --bg:#f5f7fb;
            --card:#ffffff;
            --line:#d9e1ec;
            --text:#1f2937;
            --muted:#6b7280;
            --primary:#2563eb;
            --danger:#dc2626;
            --ok:#16a34a;
            --warn:#d97706;
        }
        * { box-sizing:border-box; }
        body {
            margin:0;
            font-family: Arial, Helvetica, sans-serif;
            background:var(--bg);
            color:var(--text);
        }
        .wrap {
            display:grid;
            grid-template-columns: 240px 1fr;
            min-height:100vh;
        }
        .sidebar {
            background:#111827;
            color:white;
            padding:18px;
        }
        .sidebar h2{
            margin:0 0 18px 0;
            font-size:20px;
        }
        .sidebar a{
            display:block;
            color:#e5e7eb;
            text-decoration:none;
            padding:10px 12px;
            border-radius:10px;
            margin-bottom:6px;
        }
        .sidebar a:hover{
            background:#1f2937;
        }
        .main{
            padding:24px;
        }
        .top-title{
            display:flex;
            justify-content:space-between;
            align-items:center;
            gap:12px;
            margin-bottom:18px;
        }
        .card{
            background:var(--card);
            border:1px solid var(--line);
            border-radius:16px;
            padding:16px;
            margin-bottom:16px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.03);
        }
        .grid{
            display:grid;
            grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
            gap:14px;
        }
        .stat{
            background:var(--card);
            border:1px solid var(--line);
            border-radius:16px;
            padding:18px;
        }
        .stat .n{
            font-size:28px;
            font-weight:bold;
            margin-top:8px;
        }
        form.grid-form{
            display:grid;
            grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
            gap:12px;
        }
        label{
            display:block;
            font-size:13px;
            color:var(--muted);
            margin-bottom:6px;
        }
        input, select, textarea, button{
            width:100%;
            padding:10px 12px;
            border:1px solid var(--line);
            border-radius:10px;
            font-size:14px;
        }
        textarea{
            min-height:90px;
            resize:vertical;
        }
        button, .btn{
            background:var(--primary);
            color:white;
            border:none;
            cursor:pointer;
            text-decoration:none;
            display:inline-block;
            text-align:center;
        }
        .btn-secondary{
            background:#374151;
        }
        .btn-danger{
            background:var(--danger);
        }
        .btn-ok{
            background:var(--ok);
        }
        .btn-warn{
            background:var(--warn);
        }
        .inline-actions{
            display:flex;
            gap:8px;
            flex-wrap:wrap;
        }
        table{
            width:100%;
            border-collapse:collapse;
            background:white;
            border-radius:14px;
            overflow:hidden;
        }
        th, td{
            border-bottom:1px solid var(--line);
            padding:10px 8px;
            text-align:left;
            font-size:14px;
            vertical-align:top;
        }
        th{
            background:#eef2f7;
        }
        .badge{
            padding:4px 8px;
            border-radius:999px;
            font-size:12px;
            font-weight:bold;
            display:inline-block;
        }
        .badge-ok{ background:#dcfce7; color:#166534; }
        .badge-off{ background:#fee2e2; color:#991b1b; }
        .badge-low{ background:#fef3c7; color:#92400e; }
        .flash{
            padding:12px;
            border-radius:12px;
            margin-bottom:12px;
            border:1px solid var(--line);
            background:white;
        }
        .row-low{
            background:#fff7ed;
        }
        .muted{
            color:var(--muted);
            font-size:13px;
        }
        @media (max-width: 900px){
            .wrap{
                grid-template-columns:1fr;
            }
            .sidebar{
                position:sticky;
                top:0;
                z-index:10;
            }
        }
    </style>
</head>
<body>
<div class="wrap">
    <aside class="sidebar">
        <h2>Forrajería</h2>
        <a href="{{ url_for('index') }}">Dashboard</a>
        <a href="{{ url_for('productos') }}">Productos</a>
        <a href="{{ url_for('proveedores') }}">Proveedores</a>
        <a href="{{ url_for('registrar_compra') }}">Registrar compra</a>
        <a href="{{ url_for('registrar_venta') }}">Registrar venta</a>
        <a href="{{ url_for('stock_bajo') }}">Stock bajo</a>
        <a href="{{ url_for('stock_general') }}">Stock general</a>
        <a href="{{ url_for('historial_compras') }}">Historial compras</a>
        <a href="{{ url_for('historial_ventas') }}">Historial ventas</a>
        <a href="{{ url_for('movimientos_stock') }}">Movimientos stock</a>
    </aside>

    <main class="main">
        <div class="top-title">
            <h1 style="margin:0;">{{ titulo }}</h1>
        </div>

        {% with messages = get_flashed_messages(with_categories=true) %}
          {% if messages %}
            {% for category, message in messages %}
              <div class="flash">{{ message }}</div>
            {% endfor %}
          {% endif %}
        {% endwith %}

        {{ contenido|safe }}
    </main>
</div>
</body>
</html>
"""


def render_page(titulo, contenido, **contexto):
    return render_template_string(
        BASE_HTML,
        titulo=titulo,
        contenido=render_template_string(contenido, **contexto)
    )


def parse_float(valor, default=0.0):
    try:
        if valor is None or str(valor).strip() == "":
            return default
        return float(str(valor).replace(",", "."))
    except Exception:
        return default


# =========================
# DASHBOARD
# =========================
@app.route("/")
def index():
    r = db.resumen_dashboard()
    contenido = """
    <div class="grid">
        <div class="stat">
            <div class="muted">Productos</div>
            <div class="n">{{ r.productos }}</div>
        </div>
        <div class="stat">
            <div class="muted">Proveedores</div>
            <div class="n">{{ r.proveedores }}</div>
        </div>
        <div class="stat">
            <div class="muted">Stock bajo</div>
            <div class="n">{{ r.stock_bajo }}</div>
        </div>
        <div class="stat">
            <div class="muted">Total ventas</div>
            <div class="n">${{ '%.2f'|format(r.total_ventas) }}</div>
        </div>
        <div class="stat">
            <div class="muted">Total compras activas</div>
            <div class="n">${{ '%.2f'|format(r.total_compras) }}</div>
        </div>
    </div>
    """
    return render_page("Dashboard", contenido, r=r)


# =========================
# PRODUCTOS
# =========================
@app.route("/productos", methods=["GET", "POST"])
def productos():
    if request.method == "POST":
        try:
            codigo = request.form.get("codigo", "").strip()
            nombre = request.form.get("nombre", "").strip()
            categoria = request.form.get("categoria", "").strip()
            unidad_base = request.form.get("unidad_base", "").strip() or "UN"
            es_fraccionado = 1 if request.form.get("es_fraccionado") == "on" else 0
            peso_bolsa = parse_float(request.form.get("peso_bolsa"))
            precio_compra = parse_float(request.form.get("precio_compra"))
            precio_venta = parse_float(request.form.get("precio_venta"))
            precio_venta_bolsa = parse_float(request.form.get("precio_venta_bolsa"))
            precio_venta_kg = parse_float(request.form.get("precio_venta_kg"))
            stock = parse_float(request.form.get("stock"))
            stock_minimo = parse_float(request.form.get("stock_minimo"))

            if not codigo or not nombre:
                flash("Código y nombre son obligatorios.")
                return redirect(url_for("productos"))

            ok, msg = db.agregar_producto((
                codigo, nombre, categoria, unidad_base, es_fraccionado, peso_bolsa,
                precio_compra, precio_venta, precio_venta_bolsa, precio_venta_kg,
                stock, stock_minimo
            ))
            flash(msg)
        except Exception as e:
            flash(f"Error: {e}")
        return redirect(url_for("productos"))

    filtro = request.args.get("q", "").strip()
    ver_inactivos = request.args.get("ver_inactivos", "1") == "1"
    solo_activos = not ver_inactivos
    productos = db.obtener_productos(filtro=filtro, solo_activos=solo_activos)

    contenido = """
    <div class="card">
        <h3>Nuevo producto</h3>
        <form method="post" class="grid-form">
            <div><label>Código</label><input name="codigo" required></div>
            <div><label>Nombre</label><input name="nombre" required></div>
            <div><label>Categoría</label><input name="categoria"></div>
            <div><label>Unidad base</label><input name="unidad_base" value="UN"></div>
            <div><label>Peso bolsa</label><input name="peso_bolsa" type="number" step="0.01"></div>
            <div><label>Precio compra</label><input name="precio_compra" type="number" step="0.01"></div>
            <div><label>Precio venta</label><input name="precio_venta" type="number" step="0.01"></div>
            <div><label>Precio bolsa</label><input name="precio_venta_bolsa" type="number" step="0.01"></div>
            <div><label>Precio kg</label><input name="precio_venta_kg" type="number" step="0.01"></div>
            <div><label>Stock</label><input name="stock" type="number" step="0.01" value="0"></div>
            <div><label>Stock mínimo</label><input name="stock_minimo" type="number" step="0.01" value="0"></div>
            <div style="display:flex; align-items:end;">
                <label style="margin:0;">
                    <input type="checkbox" name="es_fraccionado" style="width:auto; margin-right:8px;"> Se vende fraccionado
                </label>
            </div>
            <div style="grid-column:1/-1;">
                <button type="submit">Guardar producto</button>
            </div>
        </form>
    </div>

    <div class="card">
        <form method="get" class="grid-form">
            <div>
                <label>Buscar</label>
                <input name="q" value="{{ filtro }}" placeholder="código, nombre o categoría">
            </div>
            <div>
                <label>Ver inactivos</label>
                <select name="ver_inactivos">
                    <option value="1" {% if ver_inactivos %}selected{% endif %}>Sí</option>
                    <option value="0" {% if not ver_inactivos %}selected{% endif %}>No</option>
                </select>
            </div>
            <div style="display:flex; align-items:end;">
                <button type="submit">Filtrar</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h3>Listado de productos</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Código</th>
                    <th>Nombre</th>
                    <th>Categoría</th>
                    <th>Estado</th>
                    <th>Frac.</th>
                    <th>Peso bolsa</th>
                    <th>P. compra</th>
                    <th>P. venta</th>
                    <th>P. bolsa</th>
                    <th>P. kg</th>
                    <th>Stock</th>
                    <th>Mínimo</th>
                    <th>Acciones</th>
                </tr>
            </thead>
            <tbody>
            {% for p in productos %}
                <tr class="{% if p['stock'] <= p['stock_minimo'] and p['activo'] == 1 %}row-low{% endif %}">
                    <td>{{ p['id'] }}</td>
                    <td>{{ p['codigo'] }}</td>
                    <td>{{ p['nombre'] }}</td>
                    <td>{{ p['categoria'] or '' }}</td>
                    <td>
                        {% if p['activo'] %}
                            <span class="badge badge-ok">ACTIVO</span>
                        {% else %}
                            <span class="badge badge-off">INACTIVO</span>
                        {% endif %}
                    </td>
                    <td>{{ 'Sí' if p['es_fraccionado'] else 'No' }}</td>
                    <td>{{ '%.2f'|format(p['peso_bolsa']) }}</td>
                    <td>{{ '%.2f'|format(p['precio_compra']) }}</td>
                    <td>{{ '%.2f'|format(p['precio_venta']) }}</td>
                    <td>{{ '%.2f'|format(p['precio_venta_bolsa']) }}</td>
                    <td>{{ '%.2f'|format(p['precio_venta_kg']) }}</td>
                    <td>{{ '%.2f'|format(p['stock']) }}</td>
                    <td>{{ '%.2f'|format(p['stock_minimo']) }}</td>
                    <td>
                        <div class="inline-actions">
                            <a class="btn btn-secondary" href="{{ url_for('editar_producto', producto_id=p['id']) }}">Editar</a>
                            <a class="btn {% if p['activo'] %}btn-warn{% else %}btn-ok{% endif %}" href="{{ url_for('toggle_producto', producto_id=p['id']) }}">
                                {% if p['activo'] %}Desactivar{% else %}Reactivar{% endif %}
                            </a>
                            <a class="btn btn-danger" href="{{ url_for('eliminar_producto', producto_id=p['id']) }}" onclick="return confirm('¿Eliminar producto?')">Eliminar</a>
                        </div>
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="14">No hay productos cargados.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Productos", contenido, productos=productos, filtro=filtro, ver_inactivos=ver_inactivos)


@app.route("/productos/<int:producto_id>/editar", methods=["GET", "POST"])
def editar_producto(producto_id):
    p = db.obtener_producto(producto_id)
    if not p:
        flash("Producto no encontrado.")
        return redirect(url_for("productos"))

    if request.method == "POST":
        codigo = request.form.get("codigo", "").strip()
        nombre = request.form.get("nombre", "").strip()
        categoria = request.form.get("categoria", "").strip()
        unidad_base = request.form.get("unidad_base", "").strip() or "UN"
        es_fraccionado = 1 if request.form.get("es_fraccionado") == "on" else 0
        peso_bolsa = parse_float(request.form.get("peso_bolsa"))
        precio_compra = parse_float(request.form.get("precio_compra"))
        precio_venta = parse_float(request.form.get("precio_venta"))
        precio_venta_bolsa = parse_float(request.form.get("precio_venta_bolsa"))
        precio_venta_kg = parse_float(request.form.get("precio_venta_kg"))
        stock = parse_float(request.form.get("stock"))
        stock_minimo = parse_float(request.form.get("stock_minimo"))

        ok, msg = db.actualizar_producto(producto_id, (
            codigo, nombre, categoria, unidad_base, es_fraccionado, peso_bolsa,
            precio_compra, precio_venta, precio_venta_bolsa, precio_venta_kg,
            stock, stock_minimo
        ))
        flash(msg)
        return redirect(url_for("productos"))

    contenido = """
    <div class="card">
        <h3>Editar producto</h3>
        <form method="post" class="grid-form">
            <div><label>Código</label><input name="codigo" value="{{ p['codigo'] }}" required></div>
            <div><label>Nombre</label><input name="nombre" value="{{ p['nombre'] }}" required></div>
            <div><label>Categoría</label><input name="categoria" value="{{ p['categoria'] or '' }}"></div>
            <div><label>Unidad base</label><input name="unidad_base" value="{{ p['unidad_base'] }}"></div>
            <div><label>Peso bolsa</label><input name="peso_bolsa" type="number" step="0.01" value="{{ p['peso_bolsa'] }}"></div>
            <div><label>Precio compra</label><input name="precio_compra" type="number" step="0.01" value="{{ p['precio_compra'] }}"></div>
            <div><label>Precio venta</label><input name="precio_venta" type="number" step="0.01" value="{{ p['precio_venta'] }}"></div>
            <div><label>Precio bolsa</label><input name="precio_venta_bolsa" type="number" step="0.01" value="{{ p['precio_venta_bolsa'] }}"></div>
            <div><label>Precio kg</label><input name="precio_venta_kg" type="number" step="0.01" value="{{ p['precio_venta_kg'] }}"></div>
            <div><label>Stock</label><input name="stock" type="number" step="0.01" value="{{ p['stock'] }}"></div>
            <div><label>Stock mínimo</label><input name="stock_minimo" type="number" step="0.01" value="{{ p['stock_minimo'] }}"></div>
            <div style="display:flex; align-items:end;">
                <label style="margin:0;">
                    <input type="checkbox" name="es_fraccionado" style="width:auto; margin-right:8px;" {% if p['es_fraccionado'] %}checked{% endif %}>
                    Se vende fraccionado
                </label>
            </div>
            <div class="inline-actions" style="grid-column:1/-1;">
                <button type="submit">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('productos') }}">Volver</a>
            </div>
        </form>
    </div>
    """
    return render_page("Editar producto", contenido, p=p)


@app.route("/productos/<int:producto_id>/toggle")
def toggle_producto(producto_id):
    p = db.obtener_producto(producto_id)
    if not p:
        flash("Producto no encontrado.")
    else:
        ok, msg = db.cambiar_estado_producto(producto_id, 0 if p["activo"] else 1)
        flash(msg)
    return redirect(url_for("productos"))


@app.route("/productos/<int:producto_id>/eliminar")
def eliminar_producto(producto_id):
    ok, msg = db.eliminar_producto(producto_id)
    flash(msg)
    return redirect(url_for("productos"))


# =========================
# PROVEEDORES
# =========================
@app.route("/proveedores", methods=["GET", "POST"])
def proveedores():
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        telefono = request.form.get("telefono", "").strip()
        direccion = request.form.get("direccion", "").strip()
        observaciones = request.form.get("observaciones", "").strip()

        if not nombre:
            flash("El nombre es obligatorio.")
            return redirect(url_for("proveedores"))

        ok, msg = db.agregar_proveedor((nombre, telefono, direccion, observaciones))
        flash(msg)
        return redirect(url_for("proveedores"))

    proveedores = db.obtener_proveedores()
    contenido = """
    <div class="card">
        <h3>Nuevo proveedor</h3>
        <form method="post" class="grid-form">
            <div><label>Nombre</label><input name="nombre" required></div>
            <div><label>Teléfono</label><input name="telefono"></div>
            <div><label>Dirección</label><input name="direccion"></div>
            <div><label>Observaciones</label><input name="observaciones"></div>
            <div style="grid-column:1/-1;"><button type="submit">Guardar proveedor</button></div>
        </form>
    </div>

    <div class="card">
        <h3>Listado de proveedores</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Nombre</th><th>Teléfono</th><th>Dirección</th><th>Observaciones</th><th>Acciones</th>
                </tr>
            </thead>
            <tbody>
            {% for p in proveedores %}
                <tr>
                    <td>{{ p['id'] }}</td>
                    <td>{{ p['nombre'] }}</td>
                    <td>{{ p['telefono'] or '' }}</td>
                    <td>{{ p['direccion'] or '' }}</td>
                    <td>{{ p['observaciones'] or '' }}</td>
                    <td>
                        <div class="inline-actions">
                            <a class="btn btn-secondary" href="{{ url_for('editar_proveedor', proveedor_id=p['id']) }}">Editar</a>
                            <a class="btn btn-danger" href="{{ url_for('eliminar_proveedor', proveedor_id=p['id']) }}" onclick="return confirm('¿Eliminar proveedor?')">Eliminar</a>
                        </div>
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="6">No hay proveedores.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Proveedores", contenido, proveedores=proveedores)


@app.route("/proveedores/<int:proveedor_id>/editar", methods=["GET", "POST"])
def editar_proveedor(proveedor_id):
    p = db.obtener_proveedor(proveedor_id)
    if not p:
        flash("Proveedor no encontrado.")
        return redirect(url_for("proveedores"))

    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        telefono = request.form.get("telefono", "").strip()
        direccion = request.form.get("direccion", "").strip()
        observaciones = request.form.get("observaciones", "").strip()

        ok, msg = db.actualizar_proveedor(proveedor_id, (nombre, telefono, direccion, observaciones))
        flash(msg)
        return redirect(url_for("proveedores"))

    contenido = """
    <div class="card">
        <h3>Editar proveedor</h3>
        <form method="post" class="grid-form">
            <div><label>Nombre</label><input name="nombre" value="{{ p['nombre'] }}" required></div>
            <div><label>Teléfono</label><input name="telefono" value="{{ p['telefono'] or '' }}"></div>
            <div><label>Dirección</label><input name="direccion" value="{{ p['direccion'] or '' }}"></div>
            <div><label>Observaciones</label><input name="observaciones" value="{{ p['observaciones'] or '' }}"></div>
            <div class="inline-actions" style="grid-column:1/-1;">
                <button type="submit">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('proveedores') }}">Volver</a>
            </div>
        </form>
    </div>
    """
    return render_page("Editar proveedor", contenido, p=p)


@app.route("/proveedores/<int:proveedor_id>/eliminar")
def eliminar_proveedor(proveedor_id):
    ok, msg = db.eliminar_proveedor(proveedor_id)
    flash(msg)
    return redirect(url_for("proveedores"))


# =========================
# REGISTRAR COMPRA
# =========================
@app.route("/compras/registrar", methods=["GET", "POST"])
def registrar_compra():
    proveedores = db.obtener_proveedores()
    productos = db.obtener_productos(solo_activos=True)

    if request.method == "POST":
        try:
            proveedor_id = int(request.form.get("proveedor_id"))
            producto_ids = request.form.getlist("producto_id")
            cantidades = request.form.getlist("cantidad")
            costos = request.form.getlist("costo_unitario")
            observaciones = request.form.get("observaciones", "").strip()

            items = []
            for i in range(len(producto_ids)):
                if not producto_ids[i]:
                    continue

                producto = db.obtener_producto(int(producto_ids[i]))
                if not producto:
                    continue

                cantidad = parse_float(cantidades[i])
                costo = parse_float(costos[i])

                if cantidad <= 0 or costo <= 0:
                    continue

                tipo_compra = "Bolsa" if producto["es_fraccionado"] else "Unidad"
                subtotal = cantidad * costo

                items.append({
                    "producto_id": producto["id"],
                    "tipo_compra": tipo_compra,
                    "cantidad": cantidad,
                    "costo_unitario": costo,
                    "subtotal": subtotal
                })

            if not items:
                flash("Cargá al menos un ítem válido.")
                return redirect(url_for("registrar_compra"))

            ok, msg = db.registrar_compra(proveedor_id, items, observaciones)
            flash(msg)
            return redirect(url_for("historial_compras"))
        except Exception as e:
            flash(f"Error al registrar compra: {e}")
            return redirect(url_for("registrar_compra"))

    contenido = """
    <div class="card">
        <h3>Registrar compra</h3>
        {% if proveedores|length == 0 %}
            <p>No hay proveedores. Primero cargá uno.</p>
        {% elif productos|length == 0 %}
            <p>No hay productos activos. Primero cargá productos.</p>
        {% else %}
        <form method="post">
            <div class="grid-form">
                <div>
                    <label>Proveedor</label>
                    <select name="proveedor_id" required>
                        {% for pr in proveedores %}
                            <option value="{{ pr['id'] }}">{{ pr['nombre'] }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div style="grid-column:1/-1;">
                    <label>Observaciones</label>
                    <textarea name="observaciones"></textarea>
                </div>
            </div>

            <div class="card" style="margin-top:16px;">
                <h4>Ítems</h4>
                <p class="muted">Podés cargar hasta 8 ítems por compra. En productos fraccionados la compra se carga en bolsas.</p>
                <table>
                    <thead>
                        <tr>
                            <th>Producto</th>
                            <th>Cantidad</th>
                            <th>Costo unitario</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for i in range(8) %}
                        <tr>
                            <td>
                                <select name="producto_id">
                                    <option value="">-- seleccionar --</option>
                                    {% for p in productos %}
                                        <option value="{{ p['id'] }}">
                                            {{ p['nombre'] }} | Stock {{ '%.2f'|format(p['stock']) }}
                                            {% if p['es_fraccionado'] %}| Bolsa {{ '%.2f'|format(p['peso_bolsa']) }} kg{% endif %}
                                        </option>
                                    {% endfor %}
                                </select>
                            </td>
                            <td><input name="cantidad" type="number" step="0.01"></td>
                            <td><input name="costo_unitario" type="number" step="0.01"></td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <button type="submit">Confirmar compra</button>
        </form>
        {% endif %}
    </div>
    """
    return render_page("Registrar compra", contenido, proveedores=proveedores, productos=productos)


# =========================
# REGISTRAR VENTA
# =========================
@app.route("/ventas/registrar", methods=["GET", "POST"])
def registrar_venta():
    productos = db.obtener_productos(solo_activos=True)

    if request.method == "POST":
        try:
            producto_ids = request.form.getlist("producto_id")
            tipos = request.form.getlist("tipo_venta")
            cantidades = request.form.getlist("cantidad")

            items = []
            for i in range(len(producto_ids)):
                if not producto_ids[i]:
                    continue

                producto = db.obtener_producto(int(producto_ids[i]))
                if not producto:
                    continue

                cantidad = parse_float(cantidades[i])
                if cantidad <= 0:
                    continue

                tipo_venta = tipos[i] if i < len(tipos) and tipos[i] else ("Bolsa" if producto["es_fraccionado"] else "Unidad")

                if producto["es_fraccionado"]:
                    if tipo_venta == "Bolsa":
                        precio = producto["precio_venta_bolsa"]
                    else:
                        precio = producto["precio_venta_kg"]
                else:
                    tipo_venta = "Unidad"
                    precio = producto["precio_venta"]

                subtotal = cantidad * precio

                items.append({
                    "producto_id": producto["id"],
                    "tipo_venta": tipo_venta,
                    "cantidad": cantidad,
                    "precio_unitario": precio,
                    "subtotal": subtotal
                })

            if not items:
                flash("Cargá al menos un ítem válido.")
                return redirect(url_for("registrar_venta"))

            ok, msg = db.registrar_venta(items)
            flash(msg)
            return redirect(url_for("historial_ventas"))
        except Exception as e:
            flash(f"Error al registrar venta: {e}")
            return redirect(url_for("registrar_venta"))

    contenido = """
    <div class="card">
        <h3>Registrar venta</h3>
        {% if productos|length == 0 %}
            <p>No hay productos activos cargados.</p>
        {% else %}
        <form method="post">
            <p class="muted">Podés cargar hasta 8 ítems por venta. En productos normales usá "Unidad". En fraccionados podés usar "Bolsa" o "Kg".</p>
            <table>
                <thead>
                    <tr>
                        <th>Producto</th>
                        <th>Tipo venta</th>
                        <th>Cantidad</th>
                    </tr>
                </thead>
                <tbody>
                    {% for i in range(8) %}
                    <tr>
                        <td>
                            <select name="producto_id">
                                <option value="">-- seleccionar --</option>
                                {% for p in productos %}
                                    <option value="{{ p['id'] }}">
                                        {{ p['nombre'] }} | Stock {{ '%.2f'|format(p['stock']) }}
                                        {% if p['es_fraccionado'] %}
                                            | Bolsa ${{ '%.2f'|format(p['precio_venta_bolsa']) }} | Kg ${{ '%.2f'|format(p['precio_venta_kg']) }}
                                        {% else %}
                                            | Unidad ${{ '%.2f'|format(p['precio_venta']) }}
                                        {% endif %}
                                    </option>
                                {% endfor %}
                            </select>
                        </td>
                        <td>
                            <select name="tipo_venta">
                                <option value="Unidad">Unidad</option>
                                <option value="Bolsa">Bolsa</option>
                                <option value="Kg">Kg</option>
                            </select>
                        </td>
                        <td><input name="cantidad" type="number" step="0.01"></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <button type="submit">Confirmar venta</button>
        </form>
        {% endif %}
    </div>
    """
    return render_page("Registrar venta", contenido, productos=productos)


# =========================
# STOCK
# =========================
@app.route("/stock-bajo")
def stock_bajo():
    rows = db.obtener_stock_bajo()
    contenido = """
    <div class="card">
        <h3>Productos con stock bajo</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Código</th><th>Nombre</th><th>Tipo</th><th>Stock</th><th>Mínimo</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr class="row-low">
                    <td>{{ r['id'] }}</td>
                    <td>{{ r['codigo'] }}</td>
                    <td>{{ r['nombre'] }}</td>
                    <td>{{ 'Kg' if r['es_fraccionado'] else r['unidad_base'] }}</td>
                    <td>{{ '%.2f'|format(r['stock']) }}</td>
                    <td>{{ '%.2f'|format(r['stock_minimo']) }}</td>
                </tr>
            {% else %}
                <tr><td colspan="6">No hay productos con stock bajo.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Stock bajo", contenido, rows=rows)


@app.route("/stock-general")
def stock_general():
    filtro = request.args.get("q", "").strip()
    ver_inactivos = request.args.get("ver_inactivos", "1") == "1"
    solo_activos = not ver_inactivos
    productos = db.obtener_productos(filtro=filtro, solo_activos=solo_activos)

    contenido = """
    <div class="card">
        <form method="get" class="grid-form">
            <div>
                <label>Buscar</label>
                <input name="q" value="{{ filtro }}" placeholder="código, nombre o categoría">
            </div>
            <div>
                <label>Ver inactivos</label>
                <select name="ver_inactivos">
                    <option value="1" {% if ver_inactivos %}selected{% endif %}>Sí</option>
                    <option value="0" {% if not ver_inactivos %}selected{% endif %}>No</option>
                </select>
            </div>
            <div style="display:flex; align-items:end;">
                <button type="submit">Filtrar</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h3>Stock general</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Código</th><th>Nombre</th><th>Categoría</th><th>Tipo</th><th>Stock actual</th><th>Stock mínimo</th><th>Estado</th>
                </tr>
            </thead>
            <tbody>
            {% for p in productos %}
                <tr class="{% if p['stock'] <= p['stock_minimo'] and p['activo'] == 1 %}row-low{% endif %}">
                    <td>{{ p['id'] }}</td>
                    <td>{{ p['codigo'] }}</td>
                    <td>{{ p['nombre'] }}</td>
                    <td>{{ p['categoria'] or '' }}</td>
                    <td>{{ 'Kg' if p['es_fraccionado'] else p['unidad_base'] }}</td>
                    <td>{{ '%.2f'|format(p['stock']) }}</td>
                    <td>{{ '%.2f'|format(p['stock_minimo']) }}</td>
                    <td>
                        {% if p['activo'] %}
                            <span class="badge badge-ok">ACTIVO</span>
                        {% else %}
                            <span class="badge badge-off">INACTIVO</span>
                        {% endif %}
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="8">No hay productos.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Stock general", contenido, productos=productos, filtro=filtro, ver_inactivos=ver_inactivos)


# =========================
# HISTORIALES
# =========================
@app.route("/historial-compras")
def historial_compras():
    rows = db.obtener_compras()
    contenido = """
    <div class="card">
        <h3>Historial de compras</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Proveedor</th><th>Total</th><th>Observaciones</th><th>Estado</th><th>Acciones</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr>
                    <td>{{ r['id'] }}</td>
                    <td>{{ r['fecha'] }}</td>
                    <td>{{ r['proveedor'] }}</td>
                    <td>${{ '%.2f'|format(r['total']) }}</td>
                    <td>{{ r['observaciones'] }}</td>
                    <td>{{ r['estado'] }}</td>
                    <td>
                        <div class="inline-actions">
                            <a class="btn btn-secondary" href="{{ url_for('detalle_compra', compra_id=r['id']) }}">Detalle</a>
                            {% if r['estado'] != 'ANULADA' %}
                                <a class="btn btn-warn" href="{{ url_for('anular_compra', compra_id=r['id']) }}" onclick="return confirm('¿Anular compra?')">Anular</a>
                            {% endif %}
                            {% if r['estado'] == 'ANULADA' %}
                                <a class="btn btn-danger" href="{{ url_for('borrar_compra', compra_id=r['id']) }}" onclick="return confirm('¿Eliminar compra anulada?')">Eliminar</a>
                            {% endif %}
                        </div>
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="7">No hay compras.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Historial compras", contenido, rows=rows)


@app.route("/historial-compras/<int:compra_id>/detalle")
def detalle_compra(compra_id):
    rows = db.obtener_detalle_compra(compra_id)
    contenido = """
    <div class="card">
        <h3>Detalle compra #{{ compra_id }}</h3>
        <table>
            <thead>
                <tr>
                    <th>Código</th><th>Producto</th><th>Tipo</th><th>Cantidad</th><th>Costo unitario</th><th>Subtotal</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr>
                    <td>{{ r['codigo'] }}</td>
                    <td>{{ r['nombre'] }}</td>
                    <td>{{ r['tipo_compra'] }}</td>
                    <td>{{ '%.2f'|format(r['cantidad']) }}</td>
                    <td>{{ '%.2f'|format(r['costo_unitario']) }}</td>
                    <td>{{ '%.2f'|format(r['subtotal']) }}</td>
                </tr>
            {% else %}
                <tr><td colspan="6">Sin detalle.</td></tr>
            {% endfor %}
            </tbody>
        </table>
        <div style="margin-top:12px;">
            <a class="btn btn-secondary" href="{{ url_for('historial_compras') }}">Volver</a>
        </div>
    </div>
    """
    return render_page(f"Detalle compra #{compra_id}", contenido, rows=rows, compra_id=compra_id)


@app.route("/historial-compras/<int:compra_id>/anular")
def anular_compra(compra_id):
    ok, msg = db.anular_compra(compra_id)
    flash(msg)
    return redirect(url_for("historial_compras"))


@app.route("/historial-compras/<int:compra_id>/eliminar")
def borrar_compra(compra_id):
    ok, msg = db.eliminar_compra(compra_id)
    flash(msg)
    return redirect(url_for("historial_compras"))


@app.route("/historial-ventas")
def historial_ventas():
    rows = db.obtener_ventas()
    contenido = """
    <div class="card">
        <h3>Historial de ventas</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Total</th><th>Acciones</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr>
                    <td>{{ r['id'] }}</td>
                    <td>{{ r['fecha'] }}</td>
                    <td>${{ '%.2f'|format(r['total']) }}</td>
                    <td>
                        <div class="inline-actions">
                            <a class="btn btn-secondary" href="{{ url_for('detalle_venta', venta_id=r['id']) }}">Detalle</a>
                            <a class="btn btn-danger" href="{{ url_for('borrar_venta', venta_id=r['id']) }}" onclick="return confirm('¿Eliminar venta y devolver stock?')">Eliminar</a>
                        </div>
                    </td>
                </tr>
            {% else %}
                <tr><td colspan="4">No hay ventas.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Historial ventas", contenido, rows=rows)


@app.route("/historial-ventas/<int:venta_id>/detalle")
def detalle_venta(venta_id):
    rows = db.obtener_detalle_venta(venta_id)
    contenido = """
    <div class="card">
        <h3>Detalle venta #{{ venta_id }}</h3>
        <table>
            <thead>
                <tr>
                    <th>Código</th><th>Producto</th><th>Tipo</th><th>Cantidad</th><th>Precio unitario</th><th>Subtotal</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr>
                    <td>{{ r['codigo'] }}</td>
                    <td>{{ r['nombre'] }}</td>
                    <td>{{ r['tipo_venta'] }}</td>
                    <td>{{ '%.2f'|format(r['cantidad']) }}</td>
                    <td>{{ '%.2f'|format(r['precio_unitario']) }}</td>
                    <td>{{ '%.2f'|format(r['subtotal']) }}</td>
                </tr>
            {% else %}
                <tr><td colspan="6">Sin detalle.</td></tr>
            {% endfor %}
            </tbody>
        </table>
        <div style="margin-top:12px;">
            <a class="btn btn-secondary" href="{{ url_for('historial_ventas') }}">Volver</a>
        </div>
    </div>
    """
    return render_page(f"Detalle venta #{venta_id}", contenido, rows=rows, venta_id=venta_id)


@app.route("/historial-ventas/<int:venta_id>/eliminar")
def borrar_venta(venta_id):
    ok, msg = db.eliminar_venta(venta_id)
    flash(msg)
    return redirect(url_for("historial_ventas"))


# =========================
# MOVIMIENTOS DE STOCK
# =========================
@app.route("/movimientos-stock")
def movimientos_stock():
    filtro = request.args.get("q", "").strip()
    rows = db.obtener_movimientos_stock(filtro)
    contenido = """
    <div class="card">
        <form method="get" class="grid-form">
            <div>
                <label>Buscar</label>
                <input name="q" value="{{ filtro }}" placeholder="producto, código, tipo o referencia">
            </div>
            <div style="display:flex; align-items:end;">
                <button type="submit">Filtrar</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h3>Movimientos de stock</h3>
        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Código</th><th>Producto</th><th>Movimiento</th><th>Referencia</th><th>Cantidad</th><th>Anterior</th><th>Resultante</th><th>Obs.</th>
                </tr>
            </thead>
            <tbody>
            {% for r in rows %}
                <tr>
                    <td>{{ r['id'] }}</td>
                    <td>{{ r['fecha'] }}</td>
                    <td>{{ r['codigo'] }}</td>
                    <td>{{ r['nombre'] }}</td>
                    <td>{{ r['tipo_movimiento'] }}</td>
                    <td>{{ r['referencia'] }}</td>
                    <td>{{ '%.2f'|format(r['cantidad']) }}</td>
                    <td>{{ '%.2f'|format(r['stock_anterior']) }}</td>
                    <td>{{ '%.2f'|format(r['stock_resultante']) }}</td>
                    <td>{{ r['observaciones'] }}</td>
                </tr>
            {% else %}
                <tr><td colspan="10">No hay movimientos.</td></tr>
            {% endfor %}
            </tbody>
        </table>
    </div>
    """
    return render_page("Movimientos stock", contenido, rows=rows, filtro=filtro)


# =========================
# RUN
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
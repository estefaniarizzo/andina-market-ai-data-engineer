"""Shared reference data for the Andina Market synthetic data generators.

Used by the Azure SQL seed (`02_seed_source_data.py`), the change simulator
(`05_simulate_source_changes.py`) and the clickstream sample generator, so the
three datasets stay coherent (same product IDs, SKUs, categories and countries).
Product IDs are assigned in catalog order (1..N) by the seed script.
"""

from decimal import Decimal

COUNTRIES = {
    "CO": {"cities": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Bucaramanga"], "weight": 0.50},
    "MX": {"cities": ["Ciudad de México", "Guadalajara", "Monterrey", "Puebla"], "weight": 0.20},
    "CL": {"cities": ["Santiago", "Valparaíso", "Concepción"], "weight": 0.12},
    "PE": {"cities": ["Lima", "Arequipa", "Cusco"], "weight": 0.11},
    "AR": {"cities": ["Buenos Aires", "Córdoba", "Rosario"], "weight": 0.07},
}

# PSE is a Colombian bank-transfer method, so it is only offered in CO.
PAYMENT_METHODS_BY_COUNTRY = {
    "CO": (["credit_card", "debit_card", "pse", "wallet"], [0.38, 0.22, 0.30, 0.10]),
    "DEFAULT": (["credit_card", "debit_card", "wallet"], [0.50, 0.32, 0.18]),
}
STORE_PAYMENT_METHODS = (["cash", "debit_card", "credit_card"], [0.35, 0.35, 0.30])

SEGMENTS = (["Regular", "Premium", "Business"], [0.72, 0.23, 0.05])
CHANNELS_BY_SEGMENT = {
    "Regular": [0.45, 0.35, 0.20],
    "Premium": [0.40, 0.50, 0.10],
    "Business": [0.75, 0.10, 0.15],
}
CHANNELS = ["web", "app", "store"]

# (sku, name, category, current_unit_price, warranty_months, description)
PRODUCT_CATALOG = [
    ("TEC-001", "Audífonos inalámbricos Andina Wave", "Tecnología", Decimal("159900.00"), 12,
     "Audífonos Bluetooth 5.3 con cancelación pasiva de ruido, 30 horas de batería con el estuche y carga USB-C."),
    ("TEC-002", "Teclado mecánico Compact Pro", "Tecnología", Decimal("289900.00"), 12,
     "Teclado mecánico 75% con switches intercambiables, iluminación RGB y conexión por cable o Bluetooth."),
    ("TEC-003", "Mouse ergonómico Flow", "Tecnología", Decimal("89900.00"), 12,
     "Mouse vertical inalámbrico de 6 botones, sensor de 4000 DPI y batería recargable de 70 días."),
    ("TEC-004", "Cargador USB-C 65W", "Tecnología", Decimal("119900.00"), 6,
     "Cargador GaN de 65W con dos puertos USB-C y uno USB-A, compatible con portátiles y celulares."),
    ("TEC-005", "Smartwatch Active 2", "Tecnología", Decimal("249900.00"), 12,
     "Reloj inteligente con GPS, monitoreo de ritmo cardiaco, resistencia al agua 5 ATM y 10 días de batería."),
    ("TEC-006", "Parlante portátil Rumba", "Tecnología", Decimal("179900.00"), 12,
     "Parlante Bluetooth resistente al agua IPX7 con 20W de potencia y 15 horas de reproducción."),
    ("TEC-007", "Power bank 20000 mAh", "Tecnología", Decimal("99900.00"), 6,
     "Batería externa de 20000 mAh con carga rápida de 22.5W y pantalla de nivel de carga."),
    ("TEC-008", "Cámara web Full HD", "Tecnología", Decimal("139900.00"), 12,
     "Cámara web 1080p con micrófono doble, enfoque automático y tapa de privacidad."),
    ("HOG-001", "Licuadora Compacta 1.5L", "Hogar", Decimal("139900.00"), 24,
     "Licuadora de 600W con vaso de vidrio de 1.5 litros, 5 velocidades y cuchillas de acero inoxidable."),
    ("HOG-002", "Set de sábanas Queen algodón", "Hogar", Decimal("129900.00"), 3,
     "Juego de sábanas 100% algodón de 200 hilos para cama Queen: sábana ajustable, plana y dos fundas."),
    ("HOG-003", "Lámpara de escritorio LED", "Hogar", Decimal("79900.00"), 12,
     "Lámpara LED con brazo articulado, tres temperaturas de color y puerto USB para carga."),
    ("HOG-004", "Botella térmica 750 ml", "Hogar", Decimal("69900.00"), 6,
     "Botella de acero inoxidable de doble pared que conserva bebidas frías 24 horas y calientes 12 horas."),
    ("HOG-005", "Organizador modular 6 cubos", "Hogar", Decimal("109900.00"), 3,
     "Organizador armable de 6 cubos en polipropileno, fácil de limpiar y de ensamblar sin herramientas."),
    ("HOG-006", "Freidora de aire 4L", "Hogar", Decimal("329900.00"), 24,
     "Freidora de aire digital de 4 litros y 1500W con 8 programas predefinidos y canasta antiadherente."),
    ("HOG-007", "Cafetera de goteo 12 tazas", "Hogar", Decimal("149900.00"), 12,
     "Cafetera programable de 12 tazas con jarra de vidrio, filtro permanente y apagado automático."),
    ("MOD-001", "Mochila urbana impermeable", "Moda", Decimal("149900.00"), 6,
     "Mochila de 25 litros en tela impermeable con compartimento acolchado para portátil de hasta 15.6 pulgadas."),
    ("MOD-002", "Chaqueta liviana unisex", "Moda", Decimal("189900.00"), 3,
     "Chaqueta rompevientos plegable, repelente al agua, disponible en tallas S a XXL."),
    ("MOD-003", "Tenis deportivos Run", "Moda", Decimal("229900.00"), 3,
     "Tenis de running con suela de espuma amortiguada y capellada en malla transpirable."),
    ("MOD-004", "Gorra ajustable clásica", "Moda", Decimal("49900.00"), 1,
     "Gorra de algodón con correa ajustable y visera curva."),
    ("MOD-005", "Billetera minimalista", "Moda", Decimal("59900.00"), 6,
     "Billetera delgada en cuero sintético con bloqueo RFID y espacio para 8 tarjetas."),
    ("MOD-006", "Gafas de sol polarizadas", "Moda", Decimal("119900.00"), 6,
     "Gafas de sol con lentes polarizados UV400 y marco liviano de policarbonato."),
    ("BEL-001", "Protector solar SPF 50", "Belleza", Decimal("64900.00"), 0,
     "Protector solar facial de amplio espectro SPF 50, toque seco, apto para piel grasa."),
    ("BEL-002", "Kit hidratación facial", "Belleza", Decimal("119900.00"), 0,
     "Kit de limpiador, tónico y crema hidratante con ácido hialurónico para rutina diaria."),
    ("BEL-003", "Secador de cabello Ion", "Belleza", Decimal("159900.00"), 12,
     "Secador de 2000W con tecnología iónica, dos velocidades, tres temperaturas y boquilla concentradora."),
    ("BEL-004", "Plancha alisadora cerámica", "Belleza", Decimal("139900.00"), 12,
     "Plancha con placas de cerámica y turmalina, temperatura ajustable hasta 230 °C."),
    ("DEP-001", "Mat de yoga antideslizante", "Deportes", Decimal("89900.00"), 3,
     "Tapete de yoga TPE de 6 mm, antideslizante por ambas caras, incluye correa de transporte."),
    ("DEP-002", "Mancuernas ajustables 10 kg", "Deportes", Decimal("199900.00"), 12,
     "Par de mancuernas ajustables de 2 a 10 kg con discos y seguros de rosca."),
    ("DEP-003", "Bandas elásticas set x5", "Deportes", Decimal("49900.00"), 3,
     "Set de 5 bandas de resistencia de látex con distintos niveles de tensión."),
    ("DEP-004", "Bicicleta estática plegable", "Deportes", Decimal("899900.00"), 12,
     "Bicicleta estática plegable con 8 niveles de resistencia magnética y monitor de calorías."),
    ("DEP-005", "Balón de fútbol N.5", "Deportes", Decimal("79900.00"), 3,
     "Balón de fútbol tamaño 5 termosellado, apto para césped natural y sintético."),
    ("LIB-001", "Agenda semanal 2026", "Papelería", Decimal("39900.00"), 0,
     "Agenda semanal y mensual 2026 con tapa dura y separadores."),
    ("LIB-002", "Cuaderno punteado A5", "Papelería", Decimal("29900.00"), 0,
     "Cuaderno A5 de 160 páginas punteadas de 100 g, ideal para bullet journal."),
    ("LIB-003", "Set de marcadores pastel", "Papelería", Decimal("35900.00"), 0,
     "Set de seis marcadores de punta biselada en tonos pastel."),
    ("LIB-004", "Calculadora científica", "Papelería", Decimal("69900.00"), 12,
     "Calculadora científica de 252 funciones con pantalla de dos líneas."),
    ("JUG-001", "Rompecabezas 1000 piezas", "Juguetes", Decimal("59900.00"), 0,
     "Rompecabezas de 1000 piezas con paisajes de los Andes."),
    ("JUG-002", "Set de bloques creativos 300 pzs", "Juguetes", Decimal("129900.00"), 0,
     "Set de 300 bloques de construcción compatibles para niños desde 6 años."),
    ("MAS-001", "Cama para mascota mediana", "Mascotas", Decimal("119900.00"), 3,
     "Cama acolchada lavable para perros o gatos de hasta 15 kg."),
    ("MAS-002", "Comedero automático", "Mascotas", Decimal("219900.00"), 12,
     "Comedero programable de 4 litros con temporizador de hasta 6 comidas al día."),
    ("MAS-003", "Rascador para gatos", "Mascotas", Decimal("149900.00"), 3,
     "Torre rascadora de 90 cm con sisal natural y casita."),
    ("TEC-009", "Tablet Andina Tab 10", "Tecnología", Decimal("799900.00"), 12,
     "Tablet de 10 pulgadas, 4 GB de RAM, 64 GB de almacenamiento y batería de 7000 mAh."),
]

# Products that are not sold anymore at seed time (still valid in historical orders).
INACTIVE_SKUS = {"LIB-001", "JUG-001"}
# Products whose price increased during the seed window (historic order lines keep the old price).
PRICE_INCREASE_SKUS = {"TEC-002", "TEC-005", "HOG-006", "MOD-003", "DEP-004", "TEC-009"}

# Support ticket templates: subject -> (priority weights low/medium/high/urgent, body templates)
TICKET_TEMPLATES = {
    "No recibí confirmación de mi pedido": (
        [0.30, 0.60, 0.10, 0.00],
        [
            "Hola, hice el pedido #{order_id} el {order_date} y no me llegó el correo de confirmación. "
            "¿Pueden confirmarme si quedó registrado?",
            "Compré {product} por la {channel} y no tengo confirmación del pedido #{order_id}. Gracias.",
        ],
    ),
    "Consulta sobre tiempos de entrega": (
        [0.40, 0.55, 0.05, 0.00],
        [
            "Quisiera saber cuándo llega mi pedido #{order_id} a {city}. Lo pedí el {order_date}.",
            "¿Cuál es el tiempo de entrega estimado para {city}? Estoy esperando el pedido #{order_id}.",
        ],
    ),
    "Pedido retrasado": (
        [0.05, 0.45, 0.45, 0.05],
        [
            "Mi pedido #{order_id} debía llegar hace varios días y sigue sin entregarse. Necesito {product} con urgencia.",
            "Llevo más de una semana esperando el pedido #{order_id}. El seguimiento no se actualiza desde el {order_date}.",
        ],
    ),
    "Producto recibido con defecto": (
        [0.00, 0.40, 0.50, 0.10],
        [
            "Recibí {product} (pedido #{order_id}) y no enciende. Solicito cambio por garantía.",
            "El producto {product} llegó con la caja golpeada y una pieza rota. Adjunto fotos. Pedido #{order_id}.",
        ],
    ),
    "Solicitud de devolución": (
        [0.10, 0.70, 0.20, 0.00],
        [
            "Quiero devolver {product} del pedido #{order_id}; no es lo que esperaba. ¿Cómo hago el proceso?",
            "Solicito la devolución del pedido #{order_id}. El producto está sin abrir.",
        ],
    ),
    "Solicitud de cambio de producto": (
        [0.25, 0.65, 0.10, 0.00],
        [
            "Necesito cambiar la talla/color de {product} del pedido #{order_id}.",
            "¿Puedo cambiar {product} por otro modelo? Pedido #{order_id}.",
        ],
    ),
    "No puedo aplicar mi cupón": (
        [0.50, 0.45, 0.05, 0.00],
        [
            "Intento usar el cupón de bienvenida en la {channel} y me dice que no es válido.",
            "El código de descuento no se aplica al carrito. ¿Está vencido?",
        ],
    ),
    "Cobro duplicado en mi tarjeta": (
        [0.00, 0.10, 0.50, 0.40],
        [
            "Me cobraron dos veces el pedido #{order_id} en la tarjeta. Necesito el reembolso urgente.",
            "Aparecen dos cargos por el mismo pedido #{order_id}. Por favor revisen, es urgente.",
        ],
    ),
    "Posible fraude en mi cuenta": (
        [0.00, 0.00, 0.30, 0.70],
        [
            "Veo un pedido que no reconozco (#{order_id}) en mi cuenta. Creo que alguien entró sin autorización. Bloqueen la cuenta.",
            "Recibí un correo de compra que no hice. Urgente: posible fraude con mi tarjeta.",
        ],
    ),
    "Actualización de dirección de entrega": (
        [0.40, 0.55, 0.05, 0.00],
        [
            "Me mudé dentro de {city} y necesito actualizar la dirección del pedido #{order_id}.",
            "¿Puedo cambiar la dirección de entrega del pedido #{order_id} antes de que salga?",
        ],
    ),
    "Reembolso no recibido": (
        [0.00, 0.35, 0.50, 0.15],
        [
            "Cancelé el pedido #{order_id} y todavía no veo el reembolso en mi cuenta.",
            "Me aprobaron la devolución de {product} hace dos semanas y el dinero no ha llegado.",
        ],
    ),
}

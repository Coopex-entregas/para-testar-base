"""adiciona coluna pagamento na tabela entrega

Revision ID: add_pagamento_001
Revises: None
Create Date: 2025-07-28 20:00:00

"""

from alembic import op
import sqlalchemy as sa

revision = 'add_pagamento_001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('entrega', sa.Column('pagamento', sa.String(length=20), nullable=True, server_default='Dinheiro'))

def downgrade() -> None:
    op.drop_column('entrega', 'pagamento')

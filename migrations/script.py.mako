"""Criar tabelas iniciais

Revision ID: 123456789abc
Revises: None
Create Date: 2025-07-28 20:00:00

"""

from alembic import op
import sqlalchemy as sa


# Identificadores da migration
revision = '123456789abc'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'cooperado',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('nome', sa.String(length=100), nullable=False, unique=True),
        sa.Column('senha_hash', sa.String(length=128), nullable=False)
    )
    op.create_table(
        'entrega',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('cliente', sa.String(length=100), nullable=False),
        sa.Column('bairro', sa.String(length=50), nullable=False),
        sa.Column('valor', sa.Float, nullable=False),
        sa.Column('data_envio', sa.DateTime, nullable=False),
        sa.Column('data_atribuida', sa.DateTime, nullable=True),
        sa.Column('cooperado_id', sa.Integer, sa.ForeignKey('cooperado.id'), nullable=True),
        sa.Column('status_pagamento', sa.String(length=20), nullable=True),
        sa.Column('status', sa.String(length=20), nullable=True),
        sa.Column('pagamento', sa.String(length=20), nullable=True, server_default='Dinheiro')
    )


def downgrade() -> None:
    op.drop_table('entrega')
    op.drop_table('cooperado')

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.Locale;

public final class DataTypeAnalyzer extends DefaultTraversalVisitor<DataType, Void> {

    private DataTypeAnalyzer() {}

    private static final DataTypeAnalyzer INSTANCE = new DataTypeAnalyzer();

    public static DataType convert(ColumnType columnType) {
        return INSTANCE.process(columnType, null);
    }

    @Override
    public DataType visitColumnType(ColumnType node, Void context) {
        String typeName = node.name();
        if (typeName == null) {
            return DataTypes.NOT_SUPPORTED;
        } else {
            return DataTypes.ofName(typeName.toLowerCase(Locale.ENGLISH));
        }
    }

    @Override
    public DataType visitObjectColumnType(ObjectColumnType node, Void context) {
        ObjectType.Builder builder = ObjectType.builder();
        for (ColumnDefinition columnDefinition : node.nestedColumns()) {
            builder.setInnerType(columnDefinition.ident(), process(columnDefinition.type(), context));
        }
        return builder.build();
    }

    @Override
    public DataType visitCollectionColumnType(CollectionColumnType node, Void context) {
        if (node.type() == ColumnType.Type.SET) {
            throw new UnsupportedOperationException("the SET dataType is currently not supported");
        }
        DataType innerType = process(node.innerType(), context);
        return new ArrayType(innerType);
    }
}

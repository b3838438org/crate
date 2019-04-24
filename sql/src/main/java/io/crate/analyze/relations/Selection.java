/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import io.crate.analyze.QuerySpec;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.List;

public class Selection implements QueriedRelation {

    private final QueriedRelation relation;
    private final Symbol condition;

    public Selection(QueriedRelation relation, Symbol condition) {
        this.relation = relation;
        this.condition = condition;
    }

    @Override
    public QuerySpec querySpec() {
        return relation.querySpec();
    }

    @Override
    public boolean isDistinct() {
        return relation.isDistinct();
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitSelection(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        return relation.getField(path, operation);
    }

    @Override
    public List<Field> fields() {
        return relation.fields();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return relation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        relation.setQualifiedName(qualifiedName);
    }
}

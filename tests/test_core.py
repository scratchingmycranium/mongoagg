import pytest
from mongoagg import AggBuilder, InvalidLookupError, InvalidStageError, Expr

def test_basic_pipeline_construction():
    builder = AggBuilder()
    pipeline = builder.match({"status": "active"}).project({"name": 1}).build()
    
    assert len(pipeline) == 2
    assert pipeline[0] == {"$match": {"status": "active"}}
    assert pipeline[1] == {"$project": {"name": 1}}

def test_match_stage():
    builder = AggBuilder()
    pipeline = builder.match({"age": {"$gt": 18}}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$match": {"age": {"$gt": 18}}}

def test_project_stage():
    builder = AggBuilder()
    pipeline = builder.project({"name": 1, "age": 1, "_id": 0}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$project": {"name": 1, "age": 1, "_id": 0}}

def test_sort_stage():
    builder = AggBuilder()
    pipeline = builder.sort({"age": 1, "name": -1}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$sort": {"age": 1, "name": -1}}

def test_limit_and_skip():
    builder = AggBuilder()
    pipeline = builder.skip(5).limit(10).build()
    
    assert len(pipeline) == 2
    assert pipeline[0] == {"$skip": 5}
    assert pipeline[1] == {"$limit": 10}

def test_unwind_stage():
    builder = AggBuilder()
    pipeline = builder.unwind("$tags").build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}

def test_group_stage():
    builder = AggBuilder()
    pipeline = builder.group("$department", total={"$sum": "$salary"}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$group": {"_id": "$department", "total": {"$sum": "$salary"}}}

def test_lookup_stage():
    builder = AggBuilder()
    pipeline = builder.lookup(
        from_="orders",
        local_field="userId",
        foreign_field="userId",
        as_="orders"
    ).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {
        "$lookup": {
            "from": "orders",
            "localField": "userId",
            "foreignField": "userId",
            "as": "orders"
        }
    }

def test_lookup_with_pipeline():
    builder = AggBuilder()
    sub_pipeline = AggBuilder().match({"status": "completed"})
    pipeline = builder.lookup(
        from_="orders",
        as_="completed_orders",
        let_={"userId": "$_id"},
        pipeline=sub_pipeline
    ).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$lookup"]["from"] == "orders"
    assert pipeline[0]["$lookup"]["as"] == "completed_orders"
    assert pipeline[0]["$lookup"]["let"] == {"userId": "$_id"}
    assert len(pipeline[0]["$lookup"]["pipeline"]) == 1

def test_invalid_lookup_error():
    builder = AggBuilder()
    with pytest.raises(InvalidLookupError):
        builder.lookup(
            from_="orders",
            as_="orders"
        ).build()

def test_invalid_stage_error():
    builder = AggBuilder()
    with pytest.raises(InvalidStageError):
        builder.add({"invalid": "stage"}).build()

def test_pipeline_serialization():
    builder = AggBuilder()
    pipeline = builder.match({"status": "active"}).project({"name": 1}).build()
    json_str = builder.to_json()
    
    assert isinstance(json_str, str)
    assert "status" in json_str
    assert "name" in json_str

def test_complex_pipeline():
    builder = AggBuilder()
    pipeline = (
        builder
        .match({"status": "active"})
        .lookup(
            from_="orders",
            local_field="userId",
            foreign_field="userId",
            as_="orders"
        )
        .unwind("$orders")
        .group("$department", 
            total_orders={"$sum": 1},
            avg_amount={"$avg": "$orders.amount"}
        )
        .sort({"total_orders": -1})
        .limit(10)
    ).build()
    
    assert len(pipeline) == 6
    assert pipeline[0]["$match"]["status"] == "active"
    assert pipeline[1]["$lookup"]["from"] == "orders"
    assert pipeline[2]["$unwind"]["path"] == "$orders"
    assert pipeline[3]["$group"]["_id"] == "$department"
    assert pipeline[4]["$sort"]["total_orders"] == -1
    assert pipeline[5]["$limit"] == 10

def test_safe_project():
    builder = AggBuilder()
    pipeline = builder.safe_project(["name", "age", "email"]).build()
    
    assert len(pipeline) == 1
    assert pipeline[0] == {"$project": {"name": 1, "age": 1, "email": 1}}

def test_add_fields():
    builder = AggBuilder()
    pipeline = builder.add_fields({
        "total": {"$add": ["$price", "$tax"]},
        "discounted": {"$multiply": ["$price", 0.9]}
    }).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$addFields"]["total"]["$add"] == ["$price", "$tax"]
    assert pipeline[0]["$addFields"]["discounted"]["$multiply"] == ["$price", 0.9]

def test_replace_root():
    builder = AggBuilder()
    pipeline = builder.replace_root({"$arrayElemAt": ["$items", 0]}).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$replaceRoot"]["newRoot"]["$arrayElemAt"] == ["$items", 0]

def test_facet():
    builder = AggBuilder()
    pipeline = builder.facet(
        categories=[{"$group": {"_id": "$category"}}],
        total=[{"$count": "total"}]
    ).build()
    
    assert len(pipeline) == 1
    assert "categories" in pipeline[0]["$facet"]
    assert "total" in pipeline[0]["$facet"]

def test_redact():
    builder = AggBuilder()
    pipeline = builder.redact({
        "$cond": {
            "if": {"$eq": ["$level", 5]},
            "then": "$$DESCEND",
            "else": "$$PRUNE"
        }
    }).build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$redact"]["$cond"]["if"]["$eq"] == ["$level", 5]

def test_comment():
    builder = AggBuilder()
    pipeline = builder.comment("Find active users").build()
    
    assert len(pipeline) == 1
    assert pipeline[0]["$comment"] == "Find active users"

def test_mock_purchase_with_details_and_user_pipeline():
    builder = AggBuilder()
    
    # Build the match stage
    builder.match({"_id": "purchase_id", "email": "test@example.com"})
    
    # Build the product lookup pipeline
    product_lookup_pipeline = AggBuilder()
    product_lookup_pipeline.match_expr(Expr.eq("$productId", "$$productId"))
    product_lookup_pipeline.project({
        "_id": 1,
        "title": 1,
        "price": 1,
        "description": 1
    })
    
    print(product_lookup_pipeline.print_pretty())

    # Add the product lookup stage
    builder.lookup(
        from_="products",
        let_={"productId": "$productId"},
        pipeline=product_lookup_pipeline,
        as_="productDetails"
    )

    # Add the product unwind stage
    builder.unwind("$productDetails")
    
    # Build the user lookup pipeline
    ulp = AggBuilder()
    ulp.match_expr(Expr.eq("$email", "$$email"))
    ulp.project({
        "_id": 1,
        "name": 1,
        "email": 1,
    })
    
    # Add the user lookup stage
    builder.lookup(
        from_="users",
        let_={"email": "$email"},
        pipeline=ulp,
        as_="userDetails"
    )

    # Add the user unwind stage
    builder.unwind("$userDetails")
    
    # Final projection
    builder.project({
        "_id": 1,
        "userDetails": 1,
        "productDetails": 1
    })
    
    # Get the final pipeline
    pipeline = builder.build()
    
    # Verify pipeline structure
    assert len(pipeline) == 6, "Pipeline should have 6 stages"
    
    # Verify match stage
    assert pipeline[0] == {
        "$match": {
            "_id": "purchase_id",
            "email": "test@example.com"
        }
    }
    
    # Verify product lookup stage
    assert pipeline[1]["$lookup"]["from"] == "products"
    assert pipeline[1]["$lookup"]["as"] == "productDetails"
    assert pipeline[1]["$lookup"]["let"] == {"productId": "$productId"}
    assert len(pipeline[1]["$lookup"]["pipeline"]) == 2
    assert pipeline[1]["$lookup"]["pipeline"][0] == {
        "$match": {"$expr": {"$eq": ["$productId", "$$productId"]}}
    }
    assert pipeline[1]["$lookup"]["pipeline"][1] == {
        "$project": {
            "_id": 1,
            "title": 1,
            "price": 1,
            "description": 1
        }
    }
    
    # Verify product unwind stage
    assert pipeline[2] == {
        "$unwind": {
            "path": "$productDetails",
            "preserveNullAndEmptyArrays": True
        }
    }
    
    # Verify user lookup stage
    assert pipeline[3]["$lookup"]["from"] == "users"
    assert pipeline[3]["$lookup"]["as"] == "userDetails"
    assert pipeline[3]["$lookup"]["let"] == {"email": "$email"}
    assert len(pipeline[3]["$lookup"]["pipeline"]) == 2
    assert pipeline[3]["$lookup"]["pipeline"][0] == {
        "$match": {"$expr": {"$eq": ["$email", "$$email"]}}
    }
    assert pipeline[3]["$lookup"]["pipeline"][1] == {
        "$project": {
            "_id": 1,
            "name": 1,
            "email": 1
        }
    }
    
    # Verify user unwind stage
    assert pipeline[4] == {
        "$unwind": {
            "path": "$userDetails",
            "preserveNullAndEmptyArrays": True
        }
    }
    
    # Verify final projection
    assert pipeline[5] == {
        "$project": {
            "_id": 1,
            "userDetails": 1,
            "productDetails": 1
        }
    }
    
    
    
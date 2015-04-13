#include "quark-internal.h"

#pragma librcd

dict(uint8_t);

static json_value_t objid(qk_ctx_t* ctx, void* ptr, fstr_t type) {
    size_t n = ((size_t) ptr - (size_t) acid_memory(ctx->ah).str) >> QK_VM_ATOM_2E;
    return jstr(sconc(type, "/", fss(fstr_from_uint(n, 16))));
}

static void qk_vis_part(
    qk_ctx_t* ctx, uint8_t level, qk_part_t* part,
    json_value_t nodes, json_value_t edges,
    json_value_t from_id, dict(uint8_t)* visited
) {
    qk_hdr_t* hdr = ctx->hdr;
    json_value_t part_id = objid(ctx, part, "part");
    json_append(edges, jobj_new(
        {"from", from_id},
        {"to", part_id},
    ));
    if (!dict_insert(visited, uint8_t, jstrv(part_id), 1))
        return;
    json_append(nodes, jobj_new(
        {"group", jstr("partition")},
        {"id", part_id},
        {"label", jstr(concs("level #", level, "\n", jstrv(part_id)))},
    ));
    qk_idx_t* idx0 = qk_part_get_idx0(part);
    qk_idx_t* idxE = idx0 + part->n_keys;
    json_value_t prev_node_id;
    for (qk_idx_t* idxC = idx0; idxC < idxE; idxC++) {
        json_value_t node_id = jstr(concs(jstrv(part_id), "/node#", (idxC - idx0)));
        json_append(edges, jobj_new(
            {"from", part_id},
            {"to", node_id},
        ));
        fstr_t key = qk_idx_get_key(idxC); // fss(fstr_hexencode(fstr_slice(qk_idx_get_key(idxC), 0, 4)));
        if (level > 0) {
            json_append(nodes, jobj_new(
                {"group", jstr("key-node")},
                {"id", node_id},
                {"label", jstr(key)},
            ));
            qk_part_t* cpart = *qk_idx1_get_down_ptr(idxC);
            qk_vis_part(ctx, level - 1, cpart, nodes, edges, node_id, visited);
        } else {
            fstr_t value = qk_idx0_get_value(idxC); //fss(fstr_hexencode(fstr_slice(qk_idx0_get_value(idxC), 0, 4)));
            json_append(nodes, jobj_new(
                {"group", jstr("value-node")},
                {"id", node_id},
                {"label", jstr(concs(key, "\n", value))},
            ));
        }
        if (idxC > idx0) {
            json_append(edges, jobj_new(
                {"from", prev_node_id},
                {"to", node_id},
                {"length", jnum(5)}
            ));
        }
        prev_node_id = node_id;
    }
}

fstr_mem_t* qk_vis_dump_graph(qk_ctx_t* ctx) { sub_heap {
    qk_hdr_t* hdr = ctx->hdr;
    json_value_t nodes = jarr_new();
    json_value_t edges = jarr_new();
    dict(uint8_t)* visited = new_dict(uint8_t);
    json_append(nodes, jobj_new(
        {"group", jstr("header")},
        {"id", jstr("header")},
        {"label", jstr("header")},
    ));
    json_append(nodes, jobj_new(
        {"group", jstr("header")},
        {"id", jstr("roots")},
        {"label", jstr("roots")},
    ));
    json_append(edges, jobj_new(
        {"from", jstr("header")},
        {"to", jstr("roots")},
    ));
    json_value_t prev_root_id;
    for (uint8_t i_lvl = 0; i_lvl < LENGTHOF(hdr->root); i_lvl++) {
        json_value_t root_id = jstr(concs("root#", i_lvl));
        json_append(nodes, jobj_new(
            {"group", jstr("root")},
            {"id", root_id},
            {"label", jstr(concs("level #", i_lvl, "\nroot"))},
        ));
        json_append(edges, jobj_new(
            {"from", jstr("roots")},
            {"to", root_id},
        ));
        qk_vis_part(ctx, i_lvl, hdr->root[i_lvl], nodes, edges, root_id, visited);
        if (i_lvl > 0) {
            json_append(edges, jobj_new(
                {"from", root_id},
                {"to", prev_root_id},
            ));
        }
        prev_root_id = root_id;
    }
    json_append(nodes, jobj_new(
        {"group", jstr("header")},
        {"id", jstr("free-lists")},
        {"label", jstr("free lists")},
    ));
    json_append(edges, jobj_new(
        {"from", jstr("header")},
        {"to", jstr("free-lists")},
    ));
    json_value_t prev_list_id;
    for (uint8_t class = 0; class < hdr->free_end_class; class++) {
        json_value_t list_id = jstr(concs("flist#", class));
        json_append(nodes, jobj_new(
            {"group", jstr("free-list")},
            {"id", list_id},
            {"label", jstr(concs("free list #", class, "\n", (1 << (QK_VM_ATOM_2E + class)), "b"))},
        ));
        json_append(edges, jobj_new(
            {"from", jstr("free-lists")},
            {"to", list_id},
        ));
        void** chunk = hdr->free_list[class];
        json_value_t prev_id = list_id;
        while (chunk != 0) {
            json_value_t cur_id = objid(ctx, chunk, "chunk");
            json_append(edges, jobj_new(
                {"from", prev_id},
                {"to", cur_id},
            ));
            json_append(nodes, jobj_new(
                {"group", jstr("free-chunk")},
                {"id", cur_id},
                {"label", cur_id},
            ));
            prev_id = cur_id;
            chunk = *chunk;
        }
        if (class > 0) {
            json_append(edges, jobj_new(
                {"from", prev_list_id},
                {"to", list_id},
            ));
        }
        prev_list_id = list_id;
    }
    return escape(json_stringify_pretty(jobj_new(
        {"nodes", nodes},
        {"edges", edges},
    )));
}}

void qk_vis_render(qk_ctx_t* ctx, rio_t* out_h, list(fstr_t)* states) { sub_heap {
    extern const fstr_t vis_tpl;
    fstr_t html = fss(fstr_replace(vis_tpl, "$DATA", concs("[", fss(fstr_implode(states, ",")), "]")));
    rio_write(out_h, html);
}}

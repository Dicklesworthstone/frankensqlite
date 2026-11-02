.bail on
.headers off
.mode quote
.parameter init
.parameter clear
.parameter set ?1 400000
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq:bound
SELECT post_id, count(*) FROM likes WHERE post_id = ?1 GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq:literal
SELECT post_id, count(*) FROM likes WHERE post_id = 400000 GROUP BY post_id;
.parameter clear
.parameter set ?1 23809
.parameter set ?2 47619
.parameter set ?3 71428
.parameter set ?4 95238
.parameter set ?5 119047
.parameter set ?6 142857
.parameter set ?7 166666
.parameter set ?8 190476
.parameter set ?9 214285
.parameter set ?10 238095
.parameter set ?11 261904
.parameter set ?12 285714
.parameter set ?13 309523
.parameter set ?14 333333
.parameter set ?15 357142
.parameter set ?16 380952
.parameter set ?17 404761
.parameter set ?18 428571
.parameter set ?19 452380
.parameter set ?20 476190
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_in20:bound
SELECT post_id, count(*) FROM likes WHERE post_id IN (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20) GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_in20:literal
SELECT post_id, count(*) FROM likes WHERE post_id IN (23809,47619,71428,95238,119047,142857,166666,190476,214285,238095,261904,285714,309523,333333,357142,380952,404761,428571,452380,476190) GROUP BY post_id;
.parameter clear
.parameter set ?1 400000
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__count_eq_control:bound
SELECT count(*) FROM likes WHERE post_id = ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__count_eq_control:literal
SELECT count(*) FROM likes WHERE post_id = 400000;
.parameter clear
.parameter set ?1 23809
.parameter set ?2 47619
.parameter set ?3 71428
.parameter set ?4 95238
.parameter set ?5 119047
.parameter set ?6 142857
.parameter set ?7 166666
.parameter set ?8 190476
.parameter set ?9 214285
.parameter set ?10 238095
.parameter set ?11 261904
.parameter set ?12 285714
.parameter set ?13 309523
.parameter set ?14 333333
.parameter set ?15 357142
.parameter set ?16 380952
.parameter set ?17 404761
.parameter set ?18 428571
.parameter set ?19 452380
.parameter set ?20 476190
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__in20_control:bound
SELECT post_id FROM likes WHERE post_id IN (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20);
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__in20_control:literal
SELECT post_id FROM likes WHERE post_id IN (23809,47619,71428,95238,119047,142857,166666,190476,214285,238095,261904,285714,309523,333333,357142,380952,404761,428571,452380,476190);
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__newest20_control:bound
SELECT p.id FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__newest20_control:literal
SELECT p.id FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT 20;
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_newest20:bound
SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_newest20:literal
SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT 20;
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__newest20_count_wrapper:bound
SELECT count(*) FROM (SELECT p.id FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT ?1);
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__newest20_count_wrapper:literal
SELECT count(*) FROM (SELECT p.id FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT 20);
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_newest20_count_wrapper:bound
SELECT count(*) FROM (SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT ?1);
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_newest20_count_wrapper:literal
SELECT count(*) FROM (SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT 20);
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__correlated_newest20:bound
SELECT p.id, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__correlated_newest20:literal
SELECT p.id, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) FROM posts p ORDER BY p.created_at DESC, p.id DESC LIMIT 20;
.parameter clear
.parameter set ?1 400000
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_by_id_control:bound
SELECT p.id, p.body, p.created_at, u.username FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_by_id_control:literal
SELECT p.id, p.body, p.created_at, u.username FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = 400000;
.parameter clear
.parameter set ?1 400000
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__correlated_by_id_control:bound
SELECT p.id, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) FROM posts p WHERE p.id = ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__correlated_by_id_control:literal
SELECT p.id, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) FROM posts p WHERE p.id = 400000;
.parameter clear
.parameter set ?1 400000
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_correlated_by_id:bound
SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) AS like_count FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_correlated_by_id:literal
SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) AS like_count FROM posts p JOIN users u ON u.id = p.user_id WHERE p.id = 400000;
.parameter clear
.parameter set ?1 20
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_correlated_newest20:bound
SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) AS like_count FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT ?1;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_correlated_newest20:literal
SELECT p.id, p.body, p.created_at, u.username, (SELECT count(*) FROM likes l WHERE l.post_id = p.id) AS like_count FROM posts p JOIN users u ON u.id = p.user_id ORDER BY p.created_at DESC, p.id DESC LIMIT 20;
.parameter clear
.parameter set ?1 20
.parameter set ?2 7
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_filtered_offset_control:bound
SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id WHERE u.id % 2 = 0 ORDER BY p.created_at DESC, p.id DESC LIMIT ?1 OFFSET ?2;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__join_filtered_offset_control:literal
SELECT p.id, u.username FROM posts p JOIN users u ON u.id = p.user_id WHERE u.id % 2 = 0 ORDER BY p.created_at DESC, p.id DESC LIMIT 20 OFFSET 7;
.parameter clear
.parameter set ?1 400000
.parameter set ?2 0
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_residual_user_id:bound
SELECT post_id, count(*) FROM likes WHERE post_id = ?1 AND user_id > ?2 GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_residual_user_id:literal
SELECT post_id, count(*) FROM likes WHERE post_id = 400000 AND user_id > 0 GROUP BY post_id;
.parameter clear
.parameter set ?1 400001
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_alternate_bind_control:bound
SELECT post_id, count(*) FROM likes WHERE post_id = ?1 GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_alternate_bind_control:literal
SELECT post_id, count(*) FROM likes WHERE post_id = 400001 GROUP BY post_id;
.parameter clear
.parameter set ?1 NULL
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_null_bind_control:bound
SELECT post_id, count(*) FROM likes WHERE post_id = ?1 GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__group_eq_null_bind_control:literal
SELECT post_id, count(*) FROM likes WHERE post_id = NULL GROUP BY post_id;
.print __ISSUE531_BOUND_STOCK_CHECK_46B67E__end

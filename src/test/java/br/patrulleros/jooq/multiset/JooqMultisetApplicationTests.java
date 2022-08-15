package br.patrulleros.jooq.multiset;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static br.patrulleros.jooq.multiset.persistence.jooq.Tables.*;
import static br.patrulleros.jooq.multiset.persistence.jooq.tables.Actor.ACTOR;
import static br.patrulleros.jooq.multiset.persistence.jooq.tables.Film.FILM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.Records.mapping;
import static org.jooq.impl.DSL.*;

@SpringBootTest
class JooqMultisetApplicationTests {

    @Autowired
    private DSLContext dslContext;

    @Test
    void contextLoads() {
        assertThat(dslContext).isNotNull();
    }

    @Test
    void withJoins() {
        var result = dslContext
                .select(
                        FILM.TITLE,
                        ACTOR.FIRST_NAME,
                        ACTOR.LAST_NAME,
                        CATEGORY.NAME)
                .from(ACTOR)
                .join(FILM_ACTOR).on(ACTOR.ACTOR_ID.eq(FILM_ACTOR.ACTOR_ID))
                .join(FILM).on(FILM_ACTOR.FILM_ID.eq(FILM.FILM_ID))
                .join(FILM_CATEGORY).on(FILM.FILM_ID.eq(FILM_CATEGORY.FILM_ID))
                .join(CATEGORY).on(FILM_CATEGORY.CATEGORY_ID.eq(CATEGORY.CATEGORY_ID))
                .orderBy(1, 2, 3, 4)
                .fetch();

        System.out.println(result);
    }

    @Test
    void withMultiset() {
        var result = dslContext
                .select(
                        FILM.TITLE,
                        multiset(
                                select(
                                        FILM_ACTOR.actor().FIRST_NAME,
                                        FILM_ACTOR.actor().LAST_NAME)
                                        .from(FILM_ACTOR)
                                        .where(FILM_ACTOR.FILM_ID.eq(FILM.FILM_ID))).as("actors"),
                        multiset(
                                select(FILM_CATEGORY.category().NAME)
                                        .from(FILM_CATEGORY)
                                        .where(FILM_CATEGORY.FILM_ID.eq(FILM.FILM_ID))).as("categories")
                )
                .from(FILM)
                .orderBy(FILM.TITLE)
                .fetch();

        System.out.println(result);
    }

    @Test
    void mappingDTOs() {
        var result = dslContext
                .select(
                        FILM.TITLE,
                        multiset(
                                select(
                                        FILM_ACTOR.actor().FIRST_NAME,
                                        FILM_ACTOR.actor().LAST_NAME)
                                        .from(FILM_ACTOR)
                                        .where(FILM_ACTOR.FILM_ID.eq(FILM.FILM_ID)))
                                .as("actors")
                                .convertFrom(r -> r.map(mapping(Actor::new))),
                        multiset(
                                select(FILM_CATEGORY.category().NAME)
                                        .from(FILM_CATEGORY)
                                        .where(FILM_CATEGORY.FILM_ID.eq(FILM.FILM_ID)))
                                .as("categories")
                                .convertFrom(r -> r.map(Record1::value1)))
                .from(FILM)
                .orderBy(FILM.TITLE)
                .fetch(mapping(Film::new));

        System.out.println(result);
    }

    record Actor(String firstName, String lastName) {
    }

    record Film(String title, List<Actor> actors, List<String> categories) {
    }

    @Test
    void complex() {
        var result = dslContext
                .select(
                        FILM.TITLE,
                        multiset(
                                select(
                                        FILM_ACTOR.actor().FIRST_NAME,
                                        FILM_ACTOR.actor().LAST_NAME)
                                        .from(FILM_ACTOR)
                                        .where(FILM_ACTOR.FILM_ID.eq(FILM.FILM_ID)))
                                .as("actors"),
                        multiset(
                                select(FILM_CATEGORY.category().NAME)
                                        .from(FILM_CATEGORY)
                                        .where(FILM_CATEGORY.FILM_ID.eq(FILM.FILM_ID)))
                                .as("categories"),
                        multiset(
                                select(
                                        PAYMENT.rental().customer().FIRST_NAME,
                                        PAYMENT.rental().customer().LAST_NAME,
                                        multisetAgg(
                                                PAYMENT.PAYMENT_DATE,
                                                PAYMENT.AMOUNT)
                                                .as("payments"),
                                        sum(PAYMENT.AMOUNT).as("total"))
                                        .from(PAYMENT)
                                        .where(PAYMENT.rental().inventory().FILM_ID.eq(FILM.FILM_ID))
                                        .groupBy(
                                                PAYMENT.rental().customer().CUSTOMER_ID,
                                                PAYMENT.rental().customer().FIRST_NAME,
                                                PAYMENT.rental().customer().LAST_NAME))
                                .as("customers"))
                .from(FILM)
                .where(FILM.TITLE.like("A%"))
                .orderBy(FILM.TITLE)
                .limit(5)
                .fetch();

        System.out.println(result);
    }
}

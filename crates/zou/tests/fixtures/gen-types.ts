export type Json =
  | string
  | number
  | boolean
  | null
  | { [key: string]: Json | undefined }
  | Json[]

export type Database = {
  public: {
    Tables: {
      "gift cards": {
        Row: {
          "2fa": boolean | null
          "full name": string
          id: number
          status: Database["public"]["Enums"]["order status"]
        }
        Insert: {
          "2fa"?: boolean | null
          "full name": string
          id: number
          status?: Database["public"]["Enums"]["order status"]
        }
        Update: {
          "2fa"?: boolean | null
          "full name"?: string
          id?: number
          status?: Database["public"]["Enums"]["order status"]
        }
        Relationships: []
      }
      members: {
        Row: {
          id: number
          pair: unknown
          progress:
            | "awaiting_payment"
            | "payment_received"
            | "preparing_to_ship"
            | "handed_to_carrier"
            | null
          secret_id: number | null
          tier: "bronze" | "silver" | "gold"
          tiers: ("bronze" | "silver" | "gold")[] | null
        }
        Insert: {
          id?: never
          pair?: unknown
          progress?:
            | "awaiting_payment"
            | "payment_received"
            | "preparing_to_ship"
            | "handed_to_carrier"
            | null
          secret_id?: number | null
          tier: "bronze" | "silver" | "gold"
          tiers?: ("bronze" | "silver" | "gold")[] | null
        }
        Update: {
          id?: never
          pair?: unknown
          progress?:
            | "awaiting_payment"
            | "payment_received"
            | "preparing_to_ship"
            | "handed_to_carrier"
            | null
          secret_id?: number | null
          tier?: "bronze" | "silver" | "gold"
          tiers?: ("bronze" | "silver" | "gold")[] | null
        }
        Relationships: []
      }
      oddities: {
        Row: {
          approximate: number | null
          at_time: string | null
          code: string | null
          flags: unknown
          id: number
          moods: Database["public"]["Enums"]["mood"][] | null
          on_day: string | null
          raw: string | null
          small: number
          spot: unknown
          when_open: string | null
        }
        Insert: {
          approximate?: number | null
          at_time?: string | null
          code?: string | null
          flags?: unknown
          id: number
          moods?: Database["public"]["Enums"]["mood"][] | null
          on_day?: string | null
          raw?: string | null
          small: number
          spot?: unknown
          when_open?: string | null
        }
        Update: {
          approximate?: number | null
          at_time?: string | null
          code?: string | null
          flags?: unknown
          id?: number
          moods?: Database["public"]["Enums"]["mood"][] | null
          on_day?: string | null
          raw?: string | null
          small?: number
          spot?: unknown
          when_open?: string | null
        }
        Relationships: []
      }
      post_stats: {
        Row: {
          post_id: string
          views: number
        }
        Insert: {
          post_id: string
          views?: number
        }
        Update: {
          post_id?: string
          views?: number
        }
        Relationships: [
          {
            foreignKeyName: "post_stats_post_id_fkey"
            columns: ["post_id"]
            isOneToOne: true
            referencedRelation: "posts"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "post_stats_post_id_fkey"
            columns: ["post_id"]
            isOneToOne: true
            referencedRelation: "published_posts"
            referencedColumns: ["id"]
          },
        ]
      }
      posts: {
        Row: {
          author_id: number
          body: string | null
          id: string
          published: boolean
          title: string
          where_when: Database["public"]["CompositeTypes"]["posted_at"] | null
        }
        Insert: {
          author_id: number
          body?: string | null
          id?: string
          published?: boolean
          title: string
          where_when?: Database["public"]["CompositeTypes"]["posted_at"] | null
        }
        Update: {
          author_id?: number
          body?: string | null
          id?: string
          published?: boolean
          title?: string
          where_when?: Database["public"]["CompositeTypes"]["posted_at"] | null
        }
        Relationships: [
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["also_id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "users"
            referencedColumns: ["id"]
          },
        ]
      }
      users: {
        Row: {
          created_at: string
          email: string | null
          handle: string
          id: number
          mood: Database["public"]["Enums"]["mood"]
          profile: Json
          score: number | null
          tags: string[] | null
          full_handle: string | null
        }
        Insert: {
          created_at?: string
          email?: string | null
          handle: string
          id?: never
          mood?: Database["public"]["Enums"]["mood"]
          profile?: Json
          score?: number | null
          tags?: string[] | null
        }
        Update: {
          created_at?: string
          email?: string | null
          handle?: string
          id?: never
          mood?: Database["public"]["Enums"]["mood"]
          profile?: Json
          score?: number | null
          tags?: string[] | null
        }
        Relationships: []
      }
    }
    Views: {
      author_ids: {
        Row: {
          also_id: number | null
          id: number | null
        }
        Insert: {
          also_id?: number | null
          id?: number | null
        }
        Update: {
          also_id?: number | null
          id?: number | null
        }
        Relationships: []
      }
      mood_counts: {
        Row: {
          mood: Database["public"]["Enums"]["mood"] | null
          people: number | null
        }
        Relationships: []
      }
      post_counts: {
        Row: {
          author_id: number | null
          posts: number | null
        }
        Relationships: [
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["also_id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "users"
            referencedColumns: ["id"]
          },
        ]
      }
      published_posts: {
        Row: {
          author_id: number | null
          id: string | null
          title: string | null
        }
        Insert: {
          author_id?: number | null
          id?: string | null
          title?: string | null
        }
        Update: {
          author_id?: number | null
          id?: string | null
          title?: string | null
        }
        Relationships: [
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["also_id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "author_ids"
            referencedColumns: ["id"]
          },
          {
            foreignKeyName: "posts_author_id_fkey"
            columns: ["author_id"]
            isOneToOne: false
            referencedRelation: "users"
            referencedColumns: ["id"]
          },
        ]
      }
    }
    Functions: {
      as_record: { Args: { a: number }; Returns: Record<string, unknown> }
      counters: { Args: never; Returns: number[] }
      dead_end: {
        Args: { "": unknown }
        Returns: {
          error: true
        } & "the function public.dead_end with parameter or with a single unnamed json/jsonb parameter, but no matches were found in the schema cache"
      }
      echo: { Args: { "": Json }; Returns: Json }
      first_post: {
        Args: { u: Database["public"]["Tables"]["users"]["Row"] }
        Returns: {
          author_id: number
          body: string | null
          id: string
          published: boolean
          title: string
          where_when: Database["public"]["CompositeTypes"]["posted_at"] | null
        }
        SetofOptions: {
          from: "users"
          to: "posts"
          isOneToOne: true
          isSetofReturn: true
        }
      }
      first_published: {
        Args: never
        Returns: {
          author_id: number | null
          id: string | null
          title: string | null
        }
        SetofOptions: {
          from: "*"
          to: "published_posts"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      full_handle: {
        Args: { "": Database["public"]["Tables"]["users"]["Row"] }
        Returns: {
          error: true
        } & "the function public.full_handle with parameter or with a single unnamed json/jsonb parameter, but no matches were found in the schema cache"
      }
      grow:
        | {
            Args: { x: number }
            Returns: {
              error: true
            } & "Could not choose the best candidate function between: public.grow(x => int4), public.grow(x => text). Try renaming the parameters or the function itself in the database so function overloading can be resolved"
          }
        | {
            Args: { x: string }
            Returns: {
              error: true
            } & "Could not choose the best candidate function between: public.grow(x => int4), public.grow(x => text). Try renaming the parameters or the function itself in the database so function overloading can be resolved"
          }
      handles: {
        Args: never
        Returns: {
          handle: string
          id: number
        }[]
      }
      hottest: { Args: never; Returns: Database["public"]["Enums"]["mood"] }
      owner: {
        Args: { p: Database["public"]["Tables"]["posts"]["Row"] }
        Returns: {
          created_at: string
          email: string | null
          handle: string
          id: number
          mood: Database["public"]["Enums"]["mood"]
          profile: Json
          score: number | null
          tags: string[] | null
        }
        SetofOptions: {
          from: "posts"
          to: "users"
          isOneToOne: true
          isSetofReturn: false
        }
      }
      pick:
        | {
            Args: never
            Returns: {
              error: true
            } & "Could not choose the best candidate function between: public.pick(), public.pick( => json). Try renaming the parameters or the function itself in the database so function overloading can be resolved"
          }
        | { Args: { a: number }; Returns: number }
        | { Args: { ""?: Json }; Returns: Json }
      post_count: {
        Args: { u: Database["public"]["Tables"]["users"]["Row"] }
        Returns: number
      }
      rename_user: {
        Args: { new_handle: string; user_id: number }
        Returns: undefined
      }
      search:
        | {
            Args: { q: string }
            Returns: {
              author_id: number
              body: string | null
              id: string
              published: boolean
              title: string
              where_when:
                | Database["public"]["CompositeTypes"]["posted_at"]
                | null
            }[]
            SetofOptions: {
              from: "*"
              to: "posts"
              isOneToOne: false
              isSetofReturn: true
            }
          }
        | {
            Args: { limit_to: number; q: string }
            Returns: {
              author_id: number
              body: string | null
              id: string
              published: boolean
              title: string
              where_when:
                | Database["public"]["CompositeTypes"]["posted_at"]
                | null
            }[]
            SetofOptions: {
              from: "*"
              to: "posts"
              isOneToOne: false
              isSetofReturn: true
            }
          }
      tally: { Args: { ns: number[] }; Returns: number }
      title_of: {
        Args: { v: Database["public"]["Views"]["published_posts"]["Row"] }
        Returns: string
      }
      top_posts: {
        Args: { limit_to?: number }
        Returns: {
          author_id: number
          body: string | null
          id: string
          published: boolean
          title: string
          where_when: Database["public"]["CompositeTypes"]["posted_at"] | null
        }[]
        SetofOptions: {
          from: "*"
          to: "posts"
          isOneToOne: false
          isSetofReturn: true
        }
      }
    }
    Enums: {
      mood: "sad" | "ok" | "happy"
      "order status":
        | "awaiting_payment"
        | "payment_received"
        | "preparing_to_ship"
        | "handed_to_carrier"
        | "delivered_to_customer"
        | "returned_by_customer"
    }
    CompositeTypes: {
      posted_at: {
        city: string | null
        at: string | null
      }
    }
  }
  shop: {
    Tables: {
      orders: {
        Row: {
          buyer_id: number
          id: number
          paid_in: Database["shop"]["Enums"]["currency"]
          total: number
        }
        Insert: {
          buyer_id: number
          id?: number
          paid_in?: Database["shop"]["Enums"]["currency"]
          total: number
        }
        Update: {
          buyer_id?: number
          id?: number
          paid_in?: Database["shop"]["Enums"]["currency"]
          total?: number
        }
        Relationships: []
      }
      users: {
        Row: {
          handle: string
          id: number
          mood: Database["shop"]["Enums"]["mood"]
        }
        Insert: {
          handle: string
          id: number
          mood?: Database["shop"]["Enums"]["mood"]
        }
        Update: {
          handle?: string
          id?: number
          mood?: Database["shop"]["Enums"]["mood"]
        }
        Relationships: []
      }
    }
    Views: {
      published_posts: {
        Row: {
          buyer_id: number | null
          id: number | null
        }
        Insert: {
          buyer_id?: number | null
          id?: number | null
        }
        Update: {
          buyer_id?: number | null
          id?: number | null
        }
        Relationships: []
      }
    }
    Functions: {
      [_ in never]: never
    }
    Enums: {
      currency: "usd" | "eur"
      mood: "cheap" | "dear"
    }
    CompositeTypes: {
      [_ in never]: never
    }
  }
}

type DatabaseWithoutInternals = Omit<Database, "__InternalSupabase">

type DefaultSchema = DatabaseWithoutInternals[Extract<keyof Database, "public">]

export type Tables<
  DefaultSchemaTableNameOrOptions extends
    | keyof (DefaultSchema["Tables"] & DefaultSchema["Views"])
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
        DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? (DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"] &
      DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Views"])[TableName] extends {
      Row: infer R
    }
    ? R
    : never
  : DefaultSchemaTableNameOrOptions extends keyof (DefaultSchema["Tables"] &
        DefaultSchema["Views"])
    ? (DefaultSchema["Tables"] &
        DefaultSchema["Views"])[DefaultSchemaTableNameOrOptions] extends {
        Row: infer R
      }
      ? R
      : never
    : never

export type TablesInsert<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Insert: infer I
    }
    ? I
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Insert: infer I
      }
      ? I
      : never
    : never

export type TablesUpdate<
  DefaultSchemaTableNameOrOptions extends
    | keyof DefaultSchema["Tables"]
    | { schema: keyof DatabaseWithoutInternals },
  TableName extends DefaultSchemaTableNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"]
    : never = never,
> = DefaultSchemaTableNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaTableNameOrOptions["schema"]]["Tables"][TableName] extends {
      Update: infer U
    }
    ? U
    : never
  : DefaultSchemaTableNameOrOptions extends keyof DefaultSchema["Tables"]
    ? DefaultSchema["Tables"][DefaultSchemaTableNameOrOptions] extends {
        Update: infer U
      }
      ? U
      : never
    : never

export type Enums<
  DefaultSchemaEnumNameOrOptions extends
    | keyof DefaultSchema["Enums"]
    | { schema: keyof DatabaseWithoutInternals },
  EnumName extends DefaultSchemaEnumNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"]
    : never = never,
> = DefaultSchemaEnumNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[DefaultSchemaEnumNameOrOptions["schema"]]["Enums"][EnumName]
  : DefaultSchemaEnumNameOrOptions extends keyof DefaultSchema["Enums"]
    ? DefaultSchema["Enums"][DefaultSchemaEnumNameOrOptions]
    : never

export type CompositeTypes<
  PublicCompositeTypeNameOrOptions extends
    | keyof DefaultSchema["CompositeTypes"]
    | { schema: keyof DatabaseWithoutInternals },
  CompositeTypeName extends PublicCompositeTypeNameOrOptions extends {
    schema: keyof DatabaseWithoutInternals
  }
    ? keyof DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"]
    : never = never,
> = PublicCompositeTypeNameOrOptions extends {
  schema: keyof DatabaseWithoutInternals
}
  ? DatabaseWithoutInternals[PublicCompositeTypeNameOrOptions["schema"]]["CompositeTypes"][CompositeTypeName]
  : PublicCompositeTypeNameOrOptions extends keyof DefaultSchema["CompositeTypes"]
    ? DefaultSchema["CompositeTypes"][PublicCompositeTypeNameOrOptions]
    : never

export const Constants = {
  public: {
    Enums: {
      mood: ["sad", "ok", "happy"],
      "order status": [
        "awaiting_payment",
        "payment_received",
        "preparing_to_ship",
        "handed_to_carrier",
        "delivered_to_customer",
        "returned_by_customer",
      ],
    },
  },
  shop: {
    Enums: {
      currency: ["usd", "eur"],
      mood: ["cheap", "dear"],
    },
  },
} as const

